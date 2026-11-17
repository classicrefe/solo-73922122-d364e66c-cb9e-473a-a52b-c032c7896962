"""LSN / WAL segment name math (pure functions, no PostgreSQL needed)."""

from app import lsn


def test_parse_and_format_roundtrip():
    for value in ("0/16B3748", "1/0", "0/FFFFFF", "ABCD/12345678"):
        assert lsn.fmt(lsn.parse(value)) == value


def test_wal_file_known_vectors():
    # postgres docs: LSN 0/16B3748 with 16MB segments -> segment 000000010000000000000001
    assert lsn.wal_file(lsn.parse("0/16B3748")) == "000000010000000000000001"
    # exact segment boundary belongs to the new segment
    assert lsn.wal_file(lsn.SEG_SIZE) == "000000010000000000000001"
    assert lsn.wal_file(0) == "000000010000000000000000"


def test_wal_files_between_is_inclusive_and_contiguous():
    start = lsn.parse("0/1000000")  # segment 1
    end = lsn.parse("0/4000000")    # segment 4
    files = lsn.wal_files_between(start, end)
    assert files == [
        "000000010000000000000001",
        "000000010000000000000002",
        "000000010000000000000003",
        "000000010000000000000004",
    ]


def test_wal_files_between_same_segment():
    files = lsn.wal_files_between(lsn.parse("0/1000000"), lsn.parse("0/1000028"))
    assert files == ["000000010000000000000001"]


def test_unreasonable_range_rejected():
    import pytest

    with pytest.raises(ValueError):
        lsn.wal_files_between(lsn.parse("F/0"), lsn.parse("0/0"))
