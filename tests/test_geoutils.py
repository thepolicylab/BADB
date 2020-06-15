from badb import geoutils


def test_create_perm():
    numbers = [1, 3, 5]
    alpha = ["A", "C"]
    actual = geoutils.create_perm(numbers, alpha)
    assert len(actual) == (
        len(numbers) * len(alpha)
        + len(alpha) * len(numbers)
        + len(numbers)
        + len(alpha)
    )
    assert set(actual) == {
        "A",
        "C",
        "1",
        "3",
        "5",
        "A1",
        "A3",
        "A5",
        "C1",
        "C3",
        "C5",
        "1A",
        "1C",
        "3A",
        "3C",
        "5A",
        "5C",
    }
