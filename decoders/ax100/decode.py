import argparse
import sys

from csp_helpers import (
    parse_header,
    verify_crc_data_only,
    verify_crc_with_header,
)


def scan_packets(raw: bytes, max_payload: int, filter_src: int | None,
                 filter_dst: int | None) -> list:
    """Scan for CSP packets, using CRC32-C to find packet boundaries."""
    results = []
    length = len(raw)
    offset = 0

    while offset <= length - 8:  # need at least header(4) + crc(4)
        hdr_bytes = raw[offset:offset + 4]
        hdr = parse_header(hdr_bytes)

        if filter_src is not None and hdr["src"] != filter_src:
            offset += 1
            continue
        if filter_dst is not None and hdr["dst"] != filter_dst:
            offset += 1
            continue

        found = False
        # Try every possible payload+crc length (min 4 bytes for CRC alone)
        for end in range(offset + 8, min(offset + 4 + max_payload + 4, length) + 1):
            payload_with_crc = raw[offset + 4:end]
            # Try both CRC modes: data-only (CSP1 default) and header-included
            if verify_crc_data_only(payload_with_crc) or verify_crc_with_header(hdr_bytes, payload_with_crc):
                actual_payload = payload_with_crc[:-4]
                total = end - offset
                results.append({
                    "offset": offset,
                    "header": hdr,
                    "header_raw": hdr_bytes,
                    "payload": actual_payload,
                    "crc_ok": True,
                    "total_len": total,
                })
                offset = end
                found = True
                break

        if not found:
            offset += 1

    return results


KISS_FEND = 0xC0
KISS_FESC = 0xDB
KISS_TFEND = 0xDC
KISS_TFESC = 0xDD


def _kiss_unescape(body: bytes) -> bytes:
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


def parse_kiss_frames(raw: bytes, filter_src: int | None,
                      filter_dst: int | None) -> list:
    """Parse KISS-framed file: each packet is FEND TYPE <escaped bytes> FEND."""
    results = []
    offset = 0
    length = len(raw)
    while offset < length:
        if raw[offset] != KISS_FEND:
            offset += 1
            continue
        start = offset
        end = raw.find(bytes([KISS_FEND]), offset + 1)
        if end == -1:
            break
        frame = raw[offset + 1:end]
        offset = end + 1
        if len(frame) < 1 + 4:
            continue
        # gr-satellites' u482c_decode emits non-data KISS frames (e.g. type 0x09
        # metadata) alongside data frames; those would parse as garbage CSP headers.
        if frame[0] & 0x0F != 0x00:
            continue
        body = _kiss_unescape(frame[1:])
        if len(body) < 4:
            continue
        hdr_bytes = body[:4]
        hdr = parse_header(hdr_bytes)
        if filter_src is not None and hdr["src"] != filter_src:
            continue
        if filter_dst is not None and hdr["dst"] != filter_dst:
            continue
        results.append({
            "offset": start,
            "header": hdr,
            "header_raw": hdr_bytes,
            "payload": body[4:],
            "crc_ok": None,
            "total_len": end - start + 1,
        })
    return results


def parse_packets_no_crc(raw: bytes, filter_src: int | None,
                         filter_dst: int | None) -> list:
    """Treat the file as a single CSP packet (header + payload, no CRC).

    Without CRC there's no way to find packet boundaries, so this assumes the
    file contains exactly one packet.
    """
    if len(raw) < 4:
        return []

    hdr_bytes = raw[:4]
    hdr = parse_header(hdr_bytes)

    if filter_src is not None and hdr["src"] != filter_src:
        return []
    if filter_dst is not None and hdr["dst"] != filter_dst:
        return []

    return [{
        "offset": 0,
        "header": hdr,
        "header_raw": hdr_bytes,
        "payload": raw[4:],
        "crc_ok": None,
        "total_len": len(raw),
    }]


def print_packet(i: int, pkt: dict, file=sys.stderr):
    h = pkt["header"]
    payload = pkt["payload"]
    print(f"Packet {i} @ offset {pkt['offset']} ({pkt['total_len']} bytes)", file=file)
    print(f"  Header : {pkt['header_raw'].hex()}", file=file)
    print(f"  prio={h['prio']} src={h['src']} dst={h['dst']} "
          f"dport={h['dport']} sport={h['sport']} "
          f"flags=0x{h['flags']:02x} [{h['flags_str']}]", file=file)
    if pkt["crc_ok"] is None:
        print(f"  CRC    : (not checked)", file=file)
    else:
        print(f"  CRC    : {'OK' if pkt['crc_ok'] else 'FAIL'}", file=file)
    print(f"  Payload: {len(payload)} bytes", file=file)

    try:
        text = payload.decode("utf-8")
        if text.isprintable() and len(text) > 0:
            print(f"  Text   : {text}", file=file)
    except (UnicodeDecodeError, ValueError):
        pass

    print(f"  Hex    : {payload.hex()}", file=file)
    print("", file=file)


def main():
    parser = argparse.ArgumentParser(
        description="Decode CSP packets from a binary file. "
        "Scans for valid packets using CRC32-C verification.")
    parser.add_argument("input", help="Input .bin file")
    parser.add_argument("--src", type=int, default=None, help="Filter by source address (0-31)")
    parser.add_argument("--dst", type=int, default=None, help="Filter by destination address (0-31)")
    parser.add_argument("--max-payload", type=int, default=256,
                        help="Maximum payload size to try (default 256)")
    crc_group = parser.add_mutually_exclusive_group()
    crc_group.add_argument("--no-crc", action="store_true",
                           help="Treat the file as a single packet without CRC "
                                "(header + payload, no boundary scanning).")
    crc_group.add_argument("--crc", action="store_true",
                           help="Force CRC mode (scan for packets with CRC32-C).")
    crc_group.add_argument("--kiss", action="store_true",
                           help="Force KISS mode (FEND-delimited frames).")
    parser.add_argument("--raw", action="store_true",
                        help="Write raw payload bytes to stdout (one per line)")
    args = parser.parse_args()

    with open(args.input, "rb") as f:
        data = f.read()

    print(f"File: {args.input} ({len(data)} bytes)", file=sys.stderr)

    if args.no_crc:
        mode = "no-crc"
        packets = parse_packets_no_crc(data, args.src, args.dst)
    elif args.crc:
        mode = "crc"
        packets = scan_packets(data, args.max_payload, args.src, args.dst)
    elif args.kiss:
        mode = "kiss"
        packets = parse_kiss_frames(data, args.src, args.dst)
    elif data.startswith(b"\xc0"):
        packets = parse_kiss_frames(data, args.src, args.dst)
        mode = "kiss (auto)"
    else:
        # Auto-detect: try CRC mode first, fall back to no-CRC if nothing found
        packets = scan_packets(data, args.max_payload, args.src, args.dst)
        if packets:
            mode = "crc (auto)"
        else:
            packets = parse_packets_no_crc(data, args.src, args.dst)
            mode = "no-crc (auto)"

    print(f"Mode: {mode}", file=sys.stderr)

    if not packets:
        print("No valid CSP packets found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(packets)} packet(s):\n", file=sys.stderr)

    for i, pkt in enumerate(packets):
        print_packet(i, pkt)
        if args.raw:
            sys.stdout.buffer.write(pkt["payload"])
            sys.stdout.buffer.write(b"\n")

    covered = sum(p["total_len"] for p in packets)
    print(f"\nCoverage: {covered}/{len(data)} bytes ({100*covered/len(data):.1f}%)", file=sys.stderr)


if __name__ == "__main__":
    main()
