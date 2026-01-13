use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use image::GenericImageView;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use walkdir::WalkDir;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input directories to search for images
    #[arg(required = true)]
    input_paths: Vec<PathBuf>,

    /// Output directory (required unless --dry-run or top command)
    #[arg(short, long)]
    out_dir: Option<PathBuf>,

    /// Number of threads to use (default: rayon global pool)
    #[arg(short, long)]
    num_cpu: Option<usize>,

    /// File extensions to process (case insensitive)
    #[arg(short, long, value_delimiter = ',', default_value = "png,jpg,jpeg")]
    extensions: Vec<String>,

    /// Dry run: do not copy/write files, just print stats
    #[arg(short, long)]
    dry_run: bool,

    /// Cache file path
    #[arg(short, long, default_value = "image_cleaner_cache_v3.json")]
    cache_file: PathBuf,

    /// Disable cache (force re-analysis)
    #[arg(long)]
    no_cache: bool,

    /// Threshold for avg pixel difference to consider a row/col as "static" (0 = disabled). Default 15.0.
    #[arg(long, default_value_t = 15.0)]
    static_threshold: f32,

    /// Minimum quality score (0-100) for Filter mode
    #[arg(short = 'm', long, default_value_t = 80.0)]
    min_score: f32,

    /// Patterns to ignore in file paths (case insensitive)
    #[arg(long, value_delimiter = ',', default_value = "unsync,filled,false_color,map")]
    ignore_patterns: Vec<String>,

    /// Minimum width/height of the clean area (0 = disabled)
    #[arg(short = 's', long, default_value_t = 0)]
    min_rect_size: u32,

    /// List top N images by quality score (overrides out_dir and other commands)
    #[arg(long)]
    top: Option<usize>,

    /// Threshold for pixel values to be considered "black" (0-255). Default 10 (tolerant).
    #[arg(long, default_value_t = 10)]
    black_threshold: u8,

    /// Minimum standard deviation of pixel brightness to be considered a valid image. Default 10.0.
    #[arg(long, default_value_t = 10.0)]
    min_std_dev: f32,

    /// Tolerance for flat-color detection (0-255). Pixel difference from mode to count as "same color". Default 15.
    #[arg(long, default_value_t = 15)]
    flat_tolerance: u8,

    /// Maximum percentage of pixels allowed to be "flat" (same color) before penalizing. Default 90.0%.
    #[arg(long, default_value_t = 90.0)]
    max_flat_pct: f32,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// Filter images based on quality score (Default)
    Filter,
    /// Crop images to the largest area free of continuous black lines
    Crop {
        /// Minimum width/height for the cropped image
        #[arg(short, long, default_value_t = 100)]
        min_size: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FileAnalysis {
    mtime: u64,
    score: f32,
    clean_rect: (u32, u32, u32, u32), // x, y, w, h
    #[serde(default)]
    std_dev: f32,
    #[serde(default)]
    black_pct: f32,
    #[serde(default)]
    flat_pct: f32,
}

#[derive(Default)]
struct Stats {
    total_files: u64,
    images_processed: u64,
    kept: u64,
    total_score: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(num) = args.num_cpu {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num)
            .build_global()
            .context("Failed to build rayon thread pool")?;
    }

    // Load cache
    let cache_path = &args.cache_file;
    let cache: HashMap<PathBuf, FileAnalysis> = if cache_path.exists() {
        let file = File::open(cache_path).context("Failed to open cache file")?;
        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Warning: Failed to parse cache file (will rebuild): {}", e);
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    };

    let extensions: Vec<String> = args
        .extensions
        .iter()
        .map(|e| e.to_lowercase())
        .collect();

    let ignore_patterns: Vec<String> = args
        .ignore_patterns
        .iter()
        .map(|p| p.to_lowercase())
        .collect();

    let mut files: Vec<(PathBuf, PathBuf)> = Vec::new();
    for input_root in &args.input_paths {
        let input_files: Vec<(PathBuf, PathBuf)> = WalkDir::new(input_root)
            .into_iter()
            .filter_map(|e| e.ok())
        .filter(|e| {
            if !e.file_type().is_file() {
                return false;
            }

            let path = e.path();
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Filter "waterfall"
            if file_name.to_lowercase().contains("waterfall") {
                return false;
            }



            // Filter extensions
            let ext = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.to_lowercase())
                .unwrap_or_default();

            extensions.contains(&ext)
        })
        .map(|e| (e.path().to_owned(), input_root.clone()))
            .collect();
        files.extend(input_files);
    }

    let stats = Arc::new(Mutex::new(Stats::default()));
    let cache_mutex = Arc::new(Mutex::new(cache));
    let top_results: Arc<Mutex<Vec<(f32, PathBuf)>>> = Arc::new(Mutex::new(Vec::new())); // Store (score, path)

    let bar = ProgressBar::new(files.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    // Force a tick to show immediately
    bar.tick();

    // Determine mode
    let command = args.command.clone().unwrap_or(Commands::Filter);

    // Validate out_dir existence (unless dry_run or top mode)
    if args.out_dir.is_none() && !args.dry_run && args.top.is_none() {
         anyhow::bail!("--out-dir is required (except for dry-run or --top)");
    }



    // Process files
    files.par_iter().for_each(|(path, root)| {
        // Stats total
        {
            let mut s = stats.lock().unwrap();
            s.total_files += 1;
        }

        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // No further checks needed here as we filtered during collection.
        // This ensures NO stats on unwanted files.

        // Check ignore patterns (with visibility)
        let path_str = path.to_string_lossy().to_lowercase();
        for pattern in &ignore_patterns {
             if path_str.contains(pattern) {
                 if args.dry_run {
                     let display_path = path.to_string_lossy().replace("\\", "/");
                     bar.println(format!(
                        "{} Pattern '{}' - \"{}\"",
                        "[SKIP]".yellow(),
                        pattern,
                        display_path
                    ));
                 }
                 bar.inc(1);
                 return;
             }
        }

        // Get mtime
        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => {
                bar.inc(1);
                return;
            }
        };
        let mtime = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check cache
        let cached_analysis = if args.no_cache {
            None
        } else {
            let cache = cache_mutex.lock().unwrap();
            if let Some(entry) = cache.get(path) {
                if entry.mtime == mtime {
                    Some(entry.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        let analysis = if let Some(a) = cached_analysis {
            a
        } else {
            // Need to process
            match analyze_image(path, &args) {
                Ok(a) => {
                    let mut entry = a;
                    entry.mtime = mtime;
                    // Update cache
                    let mut cache = cache_mutex.lock().unwrap();
                    cache.insert(path.clone(), entry.clone());
                    entry
                }
                Err(_) => {
                    bar.inc(1);
                    return;
                }
            }
        };

        {
            let mut s = stats.lock().unwrap();
            s.images_processed += 1;
            s.total_score += analysis.score as f64;
        }

        // Decide whether to keep/process
        let mut keep = if args.top.is_some() {
            analysis.score >= args.min_score
        } else {
            match &command {
                Commands::Filter => analysis.score >= args.min_score,
                Commands::Crop { min_size } => {
                     analysis.clean_rect.2 >= *min_size && analysis.clean_rect.3 >= *min_size
                }
            }
        };

        // Filter by min_rect_size (global arg)
        if keep && args.min_rect_size > 0 {
             if analysis.clean_rect.2 < args.min_rect_size || analysis.clean_rect.3 < args.min_rect_size {
                 keep = false;
             }
        }

        // Format Path: Normalizing backslashes to forward slashes for display
        let display_path = path.to_string_lossy().replace("\\", "/");

        if keep {
            // For Top mode, just collect
            if args.top.is_some() {
                 top_results.lock().unwrap().push((analysis.score, path.clone()));
                 bar.inc(1);
                 return;
            }

            stats.lock().unwrap().kept += 1;

            if args.dry_run {
                match &command {
                    Commands::Filter { .. } => {
                        bar.println(format!(
                            "{} Score: {:.2} (σ:{:.1}, B:{:.0}%, F:{:.0}%) - \"{}\"",
                            "[KEEP]".green().bold(),
                            analysis.score,
                            analysis.std_dev,
                            analysis.black_pct,
                            analysis.flat_pct,
                            display_path
                        ));
                    }
                    Commands::Crop { .. } => {
                        let (x, y, w, h) = analysis.clean_rect;
                        bar.println(format!(
                            "{} {}x{}+{},{} - \"{}\"",
                            "[CROP]".green().bold(),
                            w, h, x, y,
                            display_path
                        ));
                    }
                }
            } else if let Some(out_dir) = &args.out_dir {
                // Calculate relative path
                let relative_path = path
                    .strip_prefix(root)
                    .unwrap_or(Path::new(file_name));
                let out_path = out_dir.join(relative_path);

                if let Some(parent) = out_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }

                match &command {
                    Commands::Filter { .. } => {
                        let _ = fs::copy(path, out_path);
                    }
                    Commands::Crop { .. } => {
                       if let Ok(mut img) = image::open(path) {
                            let (x, y, w, h) = analysis.clean_rect;
                            let cropped = img.crop(x, y, w, h);
                            let _ = cropped.save(out_path);
                        }
                    }
                }
            }
        } else if args.dry_run {
             if args.top.is_some() {
                 bar.println(format!(
                    "{} Score: {:.2} (< {}) (σ:{:.1}, B:{:.0}%, F:{:.0}%) - \"{}\"",
                    "[SKIP]".yellow(),
                    analysis.score,
                    args.min_score,
                    analysis.std_dev,
                    analysis.black_pct,
                    analysis.flat_pct,
                    display_path
                ));
             } else {
                 match &command {
                        Commands::Filter => {
                             bar.println(format!(
                                "{} Score: {:.2} (< {}) (σ:{:.1}, B:{:.0}%, F:{:.0}%) - \"{}\"",
                                "[SKIP]".yellow(),
                                analysis.score,
                                args.min_score,
                                analysis.std_dev,
                                analysis.black_pct,
                                analysis.flat_pct,
                                display_path
                            ));
                        }
                        Commands::Crop { min_size } => {
                            let (x, y, w, h) = analysis.clean_rect;
                            bar.println(format!(
                                "{} {}x{}+{},{} (< {}) - \"{}\"",
                                "[SKIP]".yellow(),
                                w, h, x, y,
                                min_size,
                                display_path
                            ));
                        }
                 }
             }
        }

        bar.inc(1);
    });

    bar.finish();

    if let Some(count) = args.top {
        let mut results = top_results.lock().unwrap();
        // Sort descending by score
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then(std::cmp::Ordering::Equal));

        println!("{}", "--------------------------------".bold());
        println!("Top {} Images:", count);
        for (i, (score, path)) in results.iter().take(count).enumerate() {
            let display_path = path.to_string_lossy().replace("\\", "/");
            println!("{}. {:.2} - \"{}\"", i + 1, score, display_path);
        }
    }

    // Save cache
    let cache = cache_mutex.lock().unwrap();
    let file = File::create(cache_path).context("Failed to create cache file")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, &*cache)?;

    // Print Final Stats
    let s = stats.lock().unwrap();
    println!("{}", "--------------------------------".bold());
    println!("Processed files: {}", s.total_files);
    // println!("Skipped (waterfall): {}", s.skipped_waterfall); // Removed because we filter early now
    println!("Images analyzed: {}", s.images_processed);
    println!(
        "Kept/Copied: {} (Avg Score: {:.2})",
        s.kept.to_string().green(),
        if s.images_processed > 0 {
            s.total_score / s.images_processed as f64
        } else {
            0.0
        }
    );
    println!("{}", "--------------------------------".bold());

    Ok(())
}

fn analyze_image(path: &Path, args: &Args) -> Result<FileAnalysis> {
    let img = image::open(path)?;
    let (width, height) = img.dimensions();

    let mut row_black_count = vec![0u32; height as usize];
    let mut col_black_count = vec![0u32; width as usize];

    let mut row_noise_sum = vec![0.0; height as usize];
    let mut col_noise_sum = vec![0.0; width as usize];

    // For histogram (0-255)
    let mut histogram = vec![0u64; 256];

    let mut black_pixels = 0u64;
    let mut sum_brightness = 0u64;
    let mut sum_sq_brightness = 0u64;

    let black_thresh = args.black_threshold;
    let static_thresh = args.static_threshold;

    for y in 0..height {
        for x in 0..width {
            let p = img.get_pixel(x, y);

            // Simple brightness (average of RGB) for contrast stats
            let b_val = (p[0] as u64 + p[1] as u64 + p[2] as u64) / 3;
            sum_brightness += b_val;
            sum_sq_brightness += b_val * b_val;

            // Update histogram
            histogram[b_val as usize] += 1;

            let is_black = p[0] <= black_thresh && p[1] <= black_thresh && p[2] <= black_thresh;

            if is_black {
                row_black_count[y as usize] += 1;
                col_black_count[x as usize] += 1;
                black_pixels += 1;
            }

            if static_thresh > 0.0 {
                 if x > 0 {
                    let p_prev = img.get_pixel(x-1, y);
                    let diff = (p[0] as i16 - p_prev[0] as i16).abs() as f32 +
                               (p[1] as i16 - p_prev[1] as i16).abs() as f32 +
                               (p[2] as i16 - p_prev[2] as i16).abs() as f32;
                    row_noise_sum[y as usize] += diff / 3.0;
                }
                if y > 0 {
                    let p_prev = img.get_pixel(x, y-1);
                     let diff = (p[0] as i16 - p_prev[0] as i16).abs() as f32 +
                               (p[1] as i16 - p_prev[1] as i16).abs() as f32 +
                               (p[2] as i16 - p_prev[2] as i16).abs() as f32;
                    col_noise_sum[x as usize] += diff / 3.0;
                }
            }
        }
    }

    let mut row_is_bad = vec![false; height as usize];
    let mut col_is_bad = vec![false; width as usize];

    for y in 0..height as usize {
        let is_all_black = row_black_count[y] == width;
        let avg_noise = if width > 1 { row_noise_sum[y] / (width - 1) as f32 } else { 0.0 };
        let is_static = static_thresh > 0.0 && avg_noise > static_thresh;

        row_is_bad[y] = is_all_black || is_static;
    }

    for x in 0..width as usize {
        let is_all_black = col_black_count[x] == height;
        let avg_noise = if height > 1 { col_noise_sum[x] / (height - 1) as f32 } else { 0.0 };
        let is_static = static_thresh > 0.0 && avg_noise > static_thresh;

        col_is_bad[x] = is_all_black || is_static;
    }

    let bad_rows = row_is_bad.iter().filter(|&&b| b).count() as u64;
    let bad_cols = col_is_bad.iter().filter(|&&b| b).count() as u64;
    let total_pixels = (width as u64) * (height as u64);

    // Conservative score calculation
    let bad_structural_pixels = (bad_rows * width as u64) + (bad_cols * height as u64)
                                - (bad_rows * bad_cols);

    let structural_score = if total_pixels > 0 {
        100.0 * (1.0 - (bad_structural_pixels as f64 / total_pixels as f64))
    } else { 0.0 };

    let black_score = if total_pixels > 0 {
         100.0 * (1.0 - (black_pixels as f64 / total_pixels as f64))
    } else { 0.0 };

    let mut score = structural_score.min(black_score);

    // Std Dev Penalty (for very flat images)
    let mut std_dev_val = 0.0;
    if total_pixels > 0 {
         let mean = sum_brightness as f64 / total_pixels as f64;
         let variance = (sum_sq_brightness as f64 / total_pixels as f64) - (mean * mean);
         if variance > 0.0 {
             let std_dev = variance.sqrt();
             std_dev_val = std_dev;

             if std_dev < args.min_std_dev as f64 {
                 let penalty = if args.min_std_dev > 0.0 {
                     std_dev / args.min_std_dev as f64
                 } else { 1.0 };
                 score *= penalty;
             }
         } else if args.min_std_dev > 0.0 {
             score = 0.0;
         }
    }

    // Flatness (Histogram Mode) Penalty
    // Useful for "mostly one color with artifacts" where StdDev is skewed by artifacts
    let mut flat_pct_val = 0.0;
    if total_pixels > 0 {
        // Find Mode
        let (max_bin, _) = histogram.iter().enumerate().max_by_key(|(_, &c)| c).unwrap();

        // Sum pixels in [max_bin - tol, max_bin + tol]
        let tol = args.flat_tolerance as usize;
        let start = max_bin.saturating_sub(tol);
        let end = (max_bin + tol).min(255);

        let mut flat_pixels = 0u64;
        for i in start..=end {
            flat_pixels += histogram[i];
        }

        flat_pct_val = (flat_pixels as f64 / total_pixels as f64) * 100.0;

        if flat_pct_val > args.max_flat_pct as f64 {
             // Linear penalty from max_flat_pct (score unchanged) to 100% (score 0)
             let excess = flat_pct_val - args.max_flat_pct as f64;
             let range = 100.0 - args.max_flat_pct as f64;
             if range > 0.0 {
                 let penalty = (1.0 - (excess / range)).max(0.0);
                 score *= penalty;
             }
        }
    }

    let valid_row_intervals = get_clean_intervals(&row_is_bad);
    let valid_col_intervals = get_clean_intervals(&col_is_bad);

    let mut max_area = 0;
    let mut best_rect = (0, 0, 0, 0);

    for (r_start, r_len) in &valid_row_intervals {
        for (c_start, c_len) in &valid_col_intervals {
            let area = (r_len * c_len) as u64;
            if area > max_area {
                max_area = area;
                best_rect = (*c_start as u32, *r_start as u32, *c_len as u32, *r_len as u32);
            }
        }
    }

    let black_pct = if total_pixels > 0 {
        (black_pixels as f64 / total_pixels as f64) * 100.0
    } else { 0.0 };

    Ok(FileAnalysis {
        mtime: 0,
        score: score as f32,
        clean_rect: best_rect,
        std_dev: std_dev_val as f32,
        black_pct: black_pct as f32,
        flat_pct: flat_pct_val as f32,
    })
}

fn get_clean_intervals(is_bad: &[bool]) -> Vec<(usize, usize)> {
    let mut intervals = Vec::new();
    let mut current_start = None;

    for (i, &bad) in is_bad.iter().enumerate() {
        if !bad {
            if current_start.is_none() {
                current_start = Some(i);
            }
        } else {
            if let Some(start) = current_start {
                intervals.push((start, i - start));
                current_start = None;
            }
        }
    }

    if let Some(start) = current_start {
        intervals.push((start, is_bad.len() - start));
    }

    intervals
}


