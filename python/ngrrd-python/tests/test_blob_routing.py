"""Vetores-ouro de roteamento — DEVEM casar com o Java BlobRoutingTest."""

import pytest

from ngrrd_python.blob.spec import shard_for

# (catalog_key, shard esperado para N=64) — idêntico a BlobRoutingTest.java
GOLDEN = [
    ("series/device:r1/iface:eth0/group:traffic-v1.ngrr", 37),
    ("series/device:i-br-sc-bnu-bsh-hl5d-01/iface:int-GigabitEthernet0_1_3/group:traffic-v1.ngrr", 57),
    ("series/a.ngrr", 35),
    ("series/host=x;if=eth0.ngrr", 15),
    ("series/0.ngrr", 10),
]


@pytest.mark.parametrize("key,expected", GOLDEN)
def test_golden_vectors_match_java(key, expected):
    assert shard_for(key, 64) == expected


def test_result_within_bounds():
    for i in range(1000):
        s = shard_for(f"series/k-{i}.ngrr", 64)
        assert 0 <= s < 64


def test_rejects_non_positive_shard_count():
    with pytest.raises(ValueError):
        shard_for("series/a.ngrr", 0)
