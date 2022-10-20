import pytest

import cli


@pytest.fixture
def enriched_task():
    return cli.EnrichedCopyTask(
        source_bucket="source_bucket",
        source_key="source_key",
        destination_bucket="destination_bucket",
        destination_key="destination_key",
        total_bytes=20277918669,
        parts_count=2418,
        part_size_bytes=8388608,
    )


def test_get_part_byte_ranges(enriched_task):
    """
    Expects:

    # Part 1:    0-8388607/20277918669                  # 8388607 - 0 + 1 = 8388608 = 8 MiB
    # Part 2:    8388608-16777215/20277918669           # 16777215 - 8388608 + 1 = 8388608 = 8 MiB
    # Part 2418: 20275265536-20277918668/20277918669    # 20277918668 - 20275265536 + 1 = 2653133 ~= 2.5 KiB
    """

    gen = cli.get_part_byte_ranges(enriched_task)
    assert next(gen) == (1, 0, 8388607)
    assert next(gen) == (2, 8388608, 16777215)
    *_, last = gen
    assert last == (2418, 20275265536, 20277918668)
