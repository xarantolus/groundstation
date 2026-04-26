"""
Shared CSP helpers for the encode/decode scripts.

Uses libcsp_py3 wherever it makes sense (constants, header construction,
buffer management). The libcsp Python bindings only expose one-way operations
(build a packet -> serialize), so the inverse parsing is done here using the
documented CSP1 header layout.

CSP1 header layout (4 bytes, big-endian):
  bits[31:30] = priority (0=critical, 1=high, 2=normal, 3=low)
  bits[29:25] = source address (0-31)
  bits[24:20] = destination address (0-31)
  bits[19:14] = destination port (0-63)
  bits[13:8]  = source port (0-63)
  bits[7:0]   = flags
"""

import struct

import crcmod
import libcsp_py3 as csp

# CRC32-C function (Castagnoli polynomial), as used by CSP1 with FCRC32 flag
crc32c_fn = crcmod.predefined.mkCrcFun("crc-32c")

# XTEA was removed in libcsp 2.x; only register flags the binding exposes.
CSP_FLAG_NAMES = {
    getattr(csp, attr): name
    for attr, name in (
        ("CSP_FCRC32", "CRC32"),
        ("CSP_FHMAC", "HMAC"),
        ("CSP_FXTEA", "XTEA"),
        ("CSP_FRDP", "RDP"),
        ("CSP_FFRAG", "FRAG"),
    )
    if hasattr(csp, attr)
}


def flags_to_str(flags: int) -> str:
    """Render a CSP flags byte as a pipe-separated list of names."""
    return "|".join(name for bit, name in CSP_FLAG_NAMES.items() if flags & bit) or "NONE"


def parse_header(hdr_bytes: bytes) -> dict:
    """Parse a 4-byte CSP1 header into its fields.

    libcsp's Python bindings don't expose a header parser, so we unpack
    according to the documented bit layout.
    """
    val = struct.unpack(">I", hdr_bytes)[0]
    flags = val & 0xFF
    return {
        "prio": (val >> 30) & 0x3,
        "src": (val >> 25) & 0x1F,
        "dst": (val >> 20) & 0x1F,
        "dport": (val >> 14) & 0x3F,
        "sport": (val >> 8) & 0x3F,
        "flags": flags,
        "flags_str": flags_to_str(flags),
    }


def build_header(prio: int, src: int, dst: int, dport: int, sport: int,
                 flags: int) -> bytes:
    """Build a 4-byte CSP1 header using libcsp.

    Uses libcsp's packet_set_header / packet_get_header so the bit packing
    matches the C library exactly (no risk of drift).
    """
    # libcsp must be initialized at least once before allocating buffers
    _ensure_csp_init()
    pkt = csp.buffer_get(300)
    try:
        csp.packet_set_header(pkt, prio, src, dst, dport, sport, flags)
        return bytes(csp.packet_get_header(pkt))
    finally:
        csp.buffer_free(pkt)


_csp_initialized = False


def _ensure_csp_init() -> None:
    global _csp_initialized
    if not _csp_initialized:
        csp.init(0, "helper", "helper", "1.0", 10, 300)
        _csp_initialized = True


def has_crc(flags: int) -> bool:
    """True if the CSP packet has a 4-byte CRC32-C appended."""
    return bool(flags & csp.CSP_FCRC32)


def verify_crc_data_only(payload_with_crc: bytes) -> bool:
    """CRC32-C over payload only — libcsp 1.6 / CSP 1.x layout. Last 4 bytes
    of the input are the CRC. CSP 2.1+ would prepend the header to the
    CRC'd region, but we target libcsp 1.6 across the AX100 fleet."""
    if len(payload_with_crc) < 4:
        return False
    expected = struct.unpack(">I", payload_with_crc[-4:])[0]
    return crc32c_fn(payload_with_crc[:-4]) == expected


def verify_double_crc(header: bytes, payload_with_two_crcs: bytes) -> bool:
    """Some AX100 firmware (e.g. MIMAN) appends *two* CRC32-C trailers:
    first ``csp_crc32_append(packet, false)`` (over payload only), then
    ``csp_crc32_append(packet, true)`` (over header + payload + inner CRC).
    The wire layout becomes ``[hdr][payload][inner_crc][outer_crc]``.

    Returns True when both CRCs are present and consistent — the input is
    everything after the header, including the two 4-byte trailers."""
    if len(payload_with_two_crcs) < 8:
        return False
    inner = struct.unpack(">I", payload_with_two_crcs[-8:-4])[0]
    outer = struct.unpack(">I", payload_with_two_crcs[-4:])[0]
    if crc32c_fn(payload_with_two_crcs[:-8]) != inner:
        return False
    if crc32c_fn(header + payload_with_two_crcs[:-4]) != outer:
        return False
    return True
