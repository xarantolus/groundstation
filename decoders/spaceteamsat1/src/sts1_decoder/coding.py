"""Channel coding for the SpaceTeamSat-1 (STS1) downlink.

Mirrors STS1_COBC_SW/Sts1CobcSw/ChannelCoding:

    TM frame (223 B) -> RS(255,223) CCSDS dual basis (libfec encode_rs_ccsds)
    -> CCSDS scrambler (h(x) = x^8+x^7+x^5+x^3+1, seed 0xFF) over the 255 B block
    -> prepend ASM 1ACFFC1D
    -> r=1/2 k=7 convolutional code (G1=0o171, G2=0o133 inverted), encoder
       starts in state 0, 6 flush bits appended, padded to whole bytes
       (259 B -> 520 B, 4156 meaningful code bits)

The Si4463 then sends a short 0x55 preamble and the 520 bytes MSB first, no
radio-level sync word, CRC or whitening.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import reedsolo
from numba import njit

ASM = bytes.fromhex("1ACFFC1D")
RS_N, RS_K = 255, 223
K = 7
N_FLUSH = K - 1
G1, G2 = 0o171, 0o133  # MSB = current input bit
CADU_LEN = len(ASM) + RS_N  # 259
CODED_BITS = (CADU_LEN * 8 + N_FLUSH) * 2  # 4156
CODED_BYTES = (CODED_BITS + 7) // 8  # 520
SPACECRAFT_ID = 0x123


# --------------------------------------------------------------------------- scrambler
def _ccsds_pn(n: int) -> np.ndarray:
    a = [1] * 8
    while len(a) < n * 8:
        i = len(a) - 8
        a.append(a[i + 7] ^ a[i + 5] ^ a[i + 3] ^ a[i])
    return np.packbits(np.array(a[: n * 8], dtype=np.uint8))


PN = _ccsds_pn(RS_N)
assert bytes(PN[:8]) == bytes.fromhex("FF480EC09A0D70BC")


def scramble(block: bytes) -> bytes:
    arr = np.frombuffer(block, dtype=np.uint8)
    return (arr ^ PN[: len(arr)]).tobytes()


# --------------------------------------------------------------------------- Reed-Solomon
_TAL = [0x8D, 0xEF, 0xEC, 0x86, 0xFA, 0x99, 0xAF, 0x7B]
TALTAB = np.zeros(256, dtype=np.uint8)  # conventional -> dual basis
for _i in range(256):
    _v = 0
    for _j in range(8):
        if _i & (1 << _j):
            _v ^= _TAL[7 - _j]
    TALTAB[_i] = _v
TAL1TAB = np.zeros(256, dtype=np.uint8)  # dual -> conventional
TAL1TAB[TALTAB] = np.arange(256, dtype=np.uint8)
assert bytes(TALTAB[:4]) == bytes([0x00, 0x7B, 0xAF, 0xD4])

# libfec CCSDS: gfpoly 0x187, roots alpha^(11*(112+i)). reedsolo's `generator` is a field
# element, so pass alpha^11 = 0xAD (primitive, since gcd(11, 255) = 1).
_RS = reedsolo.RSCodec(nsym=32, nsize=255, fcr=112, prim=0x187, generator=0xAD, c_exp=8)


def rs_encode(msg: bytes, dual: bool = True) -> bytes:
    m = np.frombuffer(msg, dtype=np.uint8)
    if dual:
        m = TAL1TAB[m]
    cw = np.frombuffer(bytes(_RS.encode(m.tobytes())), dtype=np.uint8)
    if dual:
        cw = TALTAB[cw]
    return cw.tobytes()


def rs_decode(block: bytes, dual: bool = True) -> tuple[bytes, int] | None:
    """Returns (223-byte message, n corrected) or None if uncorrectable."""
    b = np.frombuffer(block, dtype=np.uint8)
    if dual:
        b = TAL1TAB[b]
    try:
        msg, _, errata = _RS.decode(b.tobytes())
    except reedsolo.ReedSolomonError:
        return None
    m = np.frombuffer(bytes(msg), dtype=np.uint8)
    if dual:
        m = TALTAB[m]
    return m.tobytes(), len(errata)


# --------------------------------------------------------------------------- convolutional
def _parity(x: int) -> int:
    return bin(x).count("1") & 1


# index = (current input << 6) | state, state bit 5 = most recent previous input
OUT = np.zeros((128, 2), dtype=np.int8)
for _idx in range(128):
    OUT[_idx, 0] = _parity(_idx & G1)
    OUT[_idx, 1] = _parity(_idx & G2) ^ 1


def conv_encode(data: bytes, flush: bool = True) -> np.ndarray:
    """Returns code bits (uint8 0/1), without the final byte padding."""
    bits = np.unpackbits(np.frombuffer(data, dtype=np.uint8))
    if flush:
        bits = np.concatenate([bits, np.zeros(N_FLUSH, dtype=np.uint8)])
    out = np.empty(2 * len(bits), dtype=np.uint8)
    state = 0
    for i, u in enumerate(bits):
        idx = (int(u) << 6) | state
        out[2 * i] = OUT[idx, 0]
        out[2 * i + 1] = OUT[idx, 1]
        state = (state >> 1) | (int(u) << 5)
    return out


def encode_frame(tm_frame: bytes, dual: bool = True) -> bytes:
    """Full STS1 TM encoding chain -> 520 on-air bytes."""
    assert len(tm_frame) == RS_K
    block = scramble(rs_encode(tm_frame, dual))
    bits = conv_encode(ASM + block)
    return np.packbits(bits).tobytes()


# Code-bit pattern of the ASM from encoder state 0 (what we search for on air).
ASM_CODED = conv_encode(ASM, flush=False)  # 64 bits
ASM_PLAIN = np.unpackbits(np.frombuffer(ASM, dtype=np.uint8))  # 32 bits (no conv)


@njit(cache=True)
def _viterbi(soft: np.ndarray, out: np.ndarray, start_state: int, end_state: int) -> np.ndarray:
    """Soft Viterbi. soft > 0 means code bit 1. Returns decoded bits (incl. flush)."""
    n = soft.shape[0] // 2
    neg = -1e30
    pm = np.full(64, neg)
    if start_state >= 0:
        pm[start_state] = 0.0
    else:
        pm[:] = 0.0
    tb = np.zeros((n, 64), dtype=np.uint8)  # predecessor state
    for t in range(n):
        y0 = soft[2 * t]
        y1 = soft[2 * t + 1]
        new = np.full(64, neg)
        for s in range(64):
            if pm[s] <= neg:
                continue
            for u in range(2):
                idx = (u << 6) | s
                m = pm[s] + y0 * (2 * out[idx, 0] - 1) + y1 * (2 * out[idx, 1] - 1)
                ns = (s >> 1) | (u << 5)
                if m > new[ns]:
                    new[ns] = m
                    tb[t, ns] = s
        pm = new
    if end_state >= 0 and pm[end_state] > neg:
        s = end_state
    else:
        s = int(np.argmax(pm))
    bits = np.zeros(n, dtype=np.uint8)
    for t in range(n - 1, -1, -1):
        bits[t] = s >> 5
        s = tb[t, s]
    return bits


def viterbi_decode(soft: np.ndarray, flushed: bool = True) -> np.ndarray:
    return _viterbi(np.ascontiguousarray(soft, dtype=np.float64), OUT, 0, 0 if flushed else -1)


# --------------------------------------------------------------------------- frame decode
@dataclass
class DecodedFrame:
    tm: bytes  # 223-byte TM transfer frame
    rs_errors: int
    dual_basis: bool
    asm_bit_errors: int
    conv_bit_errors: int  # re-encoded vs. hard decisions (channel BER estimate)

    @property
    def header(self) -> dict:
        h = self.tm
        w = int.from_bytes(h[0:2], "big")
        return {
            "version": w >> 14,
            "scid": (w >> 4) & 0x3FF,
            "vcid": (w >> 1) & 7,
            "ocf": w & 1,
            "mc_count": h[2],
            "vc_count": h[3],
            "data_status": int.from_bytes(h[4:6], "big"),
        }


def decode_coded_soft(soft: np.ndarray, conv: bool = True) -> DecodedFrame | None:
    """soft: soft code bits starting at the (coded) ASM, >0 => 1.

    conv=True expects CODED_BITS values; conv=False expects CADU_LEN*8 values.
    """
    if conv:
        bits = viterbi_decode(soft[:CODED_BITS])[: CADU_LEN * 8]
        reenc = conv_encode(np.packbits(bits).tobytes())[: CODED_BITS]
        conv_err = int(np.sum(reenc != (soft[:CODED_BITS] > 0)))
    else:
        bits = (soft[: CADU_LEN * 8] > 0).astype(np.uint8)
        conv_err = 0
    cadu = np.packbits(bits).tobytes()
    asm_err = int(np.sum(np.unpackbits(np.frombuffer(cadu[:4], np.uint8)) != ASM_PLAIN))
    block = scramble(cadu[4:])
    for dual in (True, False):
        r = rs_decode(block, dual)
        if r is not None:
            return DecodedFrame(r[0], r[1], dual, asm_err, conv_err)
    return None
