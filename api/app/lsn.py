"""WAL LSN helpers (16 MiB segments, timeline 1)."""

SEG_SIZE = 16 * 1024 * 1024
_SEGS_PER_LOG = 0x100000000 // SEG_SIZE  # 256


def parse(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def fmt(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def wal_file(lsn_int: int, timeline: int = 1) -> str:
    seg = lsn_int // SEG_SIZE
    return f"{timeline:08X}{seg // _SEGS_PER_LOG:08X}{seg % _SEGS_PER_LOG:08X}"


def wal_files_between(start_lsn: int, end_lsn: int, timeline: int = 1) -> list[str]:
    """Every WAL segment that must be archived to replay [start_lsn, end_lsn]."""
    first = start_lsn // SEG_SIZE
    last = end_lsn // SEG_SIZE
    if last < first or last - first > 100_000:
        raise ValueError(f"unreasonable WAL range: {fmt(start_lsn)}..{fmt(end_lsn)}")
    return [wal_file(s * SEG_SIZE, timeline) for s in range(first, last + 1)]
