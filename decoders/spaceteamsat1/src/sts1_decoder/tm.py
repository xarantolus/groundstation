"""KISS parsing and CCSDS TM transfer frame description."""

from __future__ import annotations

FEND, FESC, TFEND, TFESC = 0xC0, 0xDB, 0xDC, 0xDD


def parse_kiss(data: bytes) -> list[bytes]:
    frames, cur, esc = [], bytearray(), False
    for b in data:
        if b == FEND:
            if len(cur) > 1 and cur[0] == 0x00:  # data frames only (gr-satellites also emits 0x09 timestamps)
                frames.append(bytes(cur[1:]))
            cur = bytearray()
        elif esc:
            cur.append(FEND if b == TFEND else FESC if b == TFESC else b)
            esc = False
        elif b == FESC:
            esc = True
        else:
            cur.append(b)
    return frames


def tm_header(tm: bytes) -> dict:
    w = int.from_bytes(tm[0:2], "big")
    return {
        "version": w >> 14,
        "scid": (w >> 4) & 0x3FF,
        "vcid": (w >> 1) & 7,
        "ocf": w & 1,
        "mc_count": tm[2],
        "vc_count": tm[3],
        "data_status": int.from_bytes(tm[4:6], "big"),
    }


def describe_tm(tm: bytes) -> str:
    h = tm_header(tm)
    ok = "STS1" if h["scid"] == 0x123 else "scid?"
    return (
        f"[{ok}] len={len(tm)} scid=0x{h['scid']:03x} vc={h['vcid']} mc={h['mc_count']} "
        f"vcc={h['vc_count']} | {tm[:32].hex()}…"
    )
