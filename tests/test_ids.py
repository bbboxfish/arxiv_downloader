import pytest

from arxiv_downloader.errors import InvalidArxivId
from arxiv_downloader.ids import normalize_arxiv_id, normalize_unique


@pytest.mark.parametrize(
    ("raw", "base", "version", "filename"),
    [
        ("2401.01234v2", "2401.01234", 2, "2401.01234v2.pdf"),
        (
            "https://arxiv.org/abs/2401.01234v2",
            "2401.01234",
            2,
            "2401.01234v2.pdf",
        ),
        (
            "https://arxiv.org/pdf/hep-th/9901001v2.pdf",
            "hep-th/9901001",
            2,
            "hep-th__9901001v2.pdf",
        ),
    ],
)
def test_normalize_arxiv_id(raw, base, version, filename):
    identifier = normalize_arxiv_id(raw)
    assert identifier.arxiv_id == base
    assert identifier.version == version
    assert identifier.filename == filename


def test_unversioned_id_requires_resolution_before_filename():
    identifier = normalize_arxiv_id("1706.03762")
    assert identifier.version is None
    with pytest.raises(InvalidArxivId):
        _ = identifier.filename
    assert identifier.with_version(7).filename == "1706.03762v7.pdf"


@pytest.mark.parametrize(
    "value",
    ["", "not-an-id", "2401.123", "2401.01234v0", "https://example.com/2401.01234"],
)
def test_invalid_ids_are_rejected(value):
    with pytest.raises(InvalidArxivId):
        normalize_arxiv_id(value)


def test_normalize_unique_counts_duplicates_and_invalid_values():
    identifiers, duplicate_count, invalid = normalize_unique(
        ["2401.01234v1", "https://arxiv.org/abs/2401.01234v1", "bad"]
    )
    assert [item.full_id for item in identifiers] == ["2401.01234v1"]
    assert duplicate_count == 1
    assert invalid == ["bad"]
