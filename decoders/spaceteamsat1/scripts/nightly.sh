#!/usr/bin/env bash
# Nightly STS1 check: decode new SpaceTeamSat-1 recordings from today and yesterday.
#
#   scripts/nightly.sh [NAS_ROOT] [WORK_DIR]
#
# For every new pass directory with a complete recording.bin.zst:
#   decompress -> re-track to 2026-203A if recorded with other elements ->
#   sts1-survey + sts1-decode (Python) + sts1-grsat (gr-satellites) -> summary.json
# Processed passes are remembered in WORK_DIR/processed.txt; incomplete uploads
# are retried on the next run. Final stdout line per pass:
#   RESULT <pass_id> python=<n> grsat=<n> summary=<path>
set -uo pipefail

NAS_ROOT=${1:-/mnt/nas/GS}
WORK=${2:-$HOME/.cache/sts1-nightly}
PROJECT=$(cd "$(dirname "$0")/.." && pwd)
GS_ROOT=$(cd "$PROJECT/../.." && pwd)
STS1_NORAD=100609 # 2026-203A == SatNOGS/Space-Track 99416
STS1_OBJECT_ID=2026-203A

mkdir -p "$WORK/results"
touch "$WORK/processed.txt"
if [ -f "$GS_ROOT/.env" ]; then set -a; . "$GS_ROOT/.env"; set +a; fi
cd "$PROJECT"

dirs=()
for d in "$(date +%F)" "$(date -d yesterday +%F)"; do
    base="$NAS_ROOT/${d:0:4}/$d"
    [ -d "$base" ] || continue
    while IFS= read -r p; do dirs+=("$p"); done < <(find "$base" -mindepth 1 -maxdepth 1 -type d -name 'SpaceTeamSat-1_*' | sort)
done

echo "found ${#dirs[@]} SpaceTeamSat-1 pass dir(s) for today+yesterday"
for pdir in "${dirs[@]}"; do
    id=$(basename "$pdir")
    if grep -qxF "$id" "$WORK/processed.txt"; then
        echo "skip $id (already processed)"
        continue
    fi
    if [ ! -f "$pdir/recording.bin.zst" ] || [ ! -f "$pdir/info.json" ]; then
        echo "skip $id (no recording.bin.zst/info.json yet)"
        continue
    fi
    out="$WORK/results/$id"
    tmp="$WORK/tmp/$id"
    rm -rf "$tmp"; mkdir -p "$out" "$tmp"
    echo "=== $id"
    if ! zstd -q -d -f "$pdir/recording.bin.zst" -o "$tmp/recording.bin"; then
        echo "skip $id (decompression failed — upload incomplete?), will retry"
        rm -rf "$tmp"; continue
    fi
    cp "$pdir/info.json" "$pdir/doppler.txt" "$tmp/" 2>/dev/null

    rec="$tmp"
    obj=$(python3 -c "import json;print(json.load(open('$tmp/info.json'))['pass']['omm'].get('OBJECT_ID',''))")
    if [ "$obj" != "$STS1_OBJECT_ID" ] && [ -f "$tmp/doppler.txt" ] && [ -n "${LOCATION_LAT:-}" ]; then
        echo "recorded with $obj elements -> re-tracking to $STS1_OBJECT_ID"
        if uv run -q sts1-retrack "$tmp" --norad "$STS1_NORAD" --out "$tmp/retracked" > "$out/retrack.log" 2>&1; then
            rec="$tmp/retracked"
            rm -f "$tmp/recording.bin"
        else
            echo "re-track failed (see $out/retrack.log), using original"
        fi
    fi

    uv run -q sts1-survey "$rec" --out "$out/survey" --thresh-db 3 > "$out/survey.log" 2>&1
    uv run -q sts1-decode "$rec" --out "$out/decode" --f-span 30000 > "$out/decode.log" 2>&1
    uv run -q sts1-grsat "$rec" --out "$out/grsat" --f-off 0 -3000 3000 > "$out/grsat.log" 2>&1

    python3 - "$out" "$id" <<'EOF'
import json, sys, pathlib
out, pid = pathlib.Path(sys.argv[1]), sys.argv[2]
dec = json.loads((out / "decode/results.json").read_text()) if (out / "decode/results.json").exists() else []
py = [r for r in dec if r["decoded"]]
gr_lines = [l.strip() for l in (out / "grsat.log").read_text().splitlines() if l.strip().startswith("[")] if (out / "grsat.log").exists() else []
bursts = (out / "survey/bursts.txt").read_text().splitlines() if (out / "survey/bursts.txt").exists() else []
summary = {"pass_id": pid, "python_frames": py, "grsat_frames": gr_lines, "survey_bursts": bursts}
(out / "summary.json").write_text(json.dumps(summary, indent=1))
print(f"RESULT {pid} python={len(py)} grsat={len(gr_lines)} bursts={len(bursts)} summary={out / 'summary.json'}")
EOF
    rm -rf "$tmp"
    echo "$id" >> "$WORK/processed.txt"
done
echo "nightly done"
