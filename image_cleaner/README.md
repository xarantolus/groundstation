# Image Cleaner

A high-performance Rust CLI tool to filter and crop satellite images based on quality. It detects and handles signal dropouts (black lines) and static noise.

## Usage

```bash
cargo run --release -- [OPTIONS] <INPUT_PATHS>... [COMMAND]
```

### Options

*   `-m, --min-score <FLOAT>`: Minimum quality score (0-100). Default: `80.0`. (Affects **Filter** and **Top** modes).
*   `-o, --out-dir <PATH>`: Output directory for processed images (required unless `--dry-run` or `--top`).
*   `-d, --dry-run`: Scan files and print stats without modifying/copying.
*   `--top <INT>`: List the top `N` images by quality score. (Overrides other commands).
*   `--min-rect-size <INT>`: global filter: minimum clean rectangle width/height required. Default `0` (disabled).
*   `--static-threshold <FLOAT>`: Detect "static" (noisy) lines. Threshold is average pixel difference (e.g., `10.0`). Default `0.0` (disabled).
*   `--black-threshold <INT>`: Max brightness (0-255) for a pixel to count as "black". Default `0`.
*   `--min-std-dev <FLOAT>`: Penalize images with low contrast (standard deviation < value). Default `10.0`.
*   `--flat-tolerance <INT>`: Pixel value difference from the primary color mode to be considered "flat". Default `15`.
*   `--max-flat-pct <FLOAT>`: Max percentage of pixels allowed to be "flat" before penalizing. Default `90.0`.
*   `--ignore-patterns <STR>`: Comma-separated list of path substrings to ignore (case-insensitive). Default: `filled`.
*   `-e, --extensions <EXT>`: Comma-separated list of extensions (default: `png,jpg,jpeg`).
*   `-n, --num-cpu <INT>`: Number of threads (default: all cores).

### Commands

*   `filter`: Filter images based on quality score (Default mode if no command specified).
*   `crop`: Crop the largest area free of black lines/noise.
    *   `--min-size <INT>`: Minimum width/height for the crop. Default: `100`.

## Examples

**1. Dry Run (Check what would happen)**
```bash
cargo run --release -- -d ../examples/
```

**2. Filter Good Images (>90%) from Multiple Sources**
Copies images with <10% black/bad pixels to `out/`.
```bash
cargo run --release -- -m 90 -o out/ input_dir1/ input_dir2/
```

**3. List Top 10 Images**
Finds the best quality images across directories.
```bash
cargo run --release -- input_dir/ --top 10
```

**4. Filter by Minimum Clean Area Size**
Only keep images that have a clean area of at least 500x500 pixels.
```bash
cargo run --release -- --min-rect-size 500 -o out/ input_dir/
```

**5. Crop Clean Areas**
Finds the largest rectangle free of black lines/static and saves it. Rejects crops smaller than 200px.
```bash
cargo run --release -- -o out_cropped/ ../examples/ crop --min-size 200
```

**6. Filter with Static Detection**
Reject images with too much static noise using a threshold of 20.
```bash
cargo run --release -- --static-threshold 20.0 -d ../examples/
```
