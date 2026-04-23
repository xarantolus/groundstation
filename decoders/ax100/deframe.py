"""Split a KISS-framed file into one CSP packet per output file."""

import argparse
import os
import sys

KISS_FEND = 0xC0
KISS_FESC = 0xDB
KISS_TFEND = 0xDC
KISS_TFESC = 0xDD


def kiss_unescape(body: bytes) -> bytes:
    out = bytearray()
    i = 0
    while i < len(body):
        b = body[i]
        if b == KISS_FESC and i + 1 < len(body):
            nxt = body[i + 1]
            out.append(KISS_FEND if nxt == KISS_TFEND else
                       KISS_FESC if nxt == KISS_TFESC else nxt)
            i += 2
        else:
            out.append(b)
            i += 1
    return bytes(out)


def iter_data_frames(raw: bytes):
    """Yield the CSP packet bytes from each KISS data frame (type 0x00)."""
    offset = 0
    length = len(raw)
    while offset < length:
        if raw[offset] != KISS_FEND:
            offset += 1
            continue
        end = raw.find(bytes([KISS_FEND]), offset + 1)
        if end == -1:
            break
        frame = raw[offset + 1:end]
        offset = end + 1
        if not frame:
            continue
        if frame[0] & 0x0F != 0x00:
            continue
        yield kiss_unescape(frame[1:])


def main():
    parser = argparse.ArgumentParser(
        description="Split a KISS-framed file into one CSP packet per output file.")
    parser.add_argument("input", help="KISS-framed input file")
    parser.add_argument("output_dir", help="Directory to write csp_packet_N.bin files into")
    parser.add_argument("--prefix", default="csp_packet_",
                        help="Output filename prefix (default: csp_packet_)")
    args = parser.parse_args()

    with open(args.input, "rb") as f:
        raw = f.read()

    os.makedirs(args.output_dir, exist_ok=True)

    count = 0
    for pkt in iter_data_frames(raw):
        if not pkt:
            continue
        path = os.path.join(args.output_dir, f"{args.prefix}{count}.bin")
        with open(path, "wb") as f:
            f.write(pkt)
        count += 1

    print(f"Wrote {count} packet(s) to {args.output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
