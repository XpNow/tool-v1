from app.util import parse_int_ro


def test_parse_int_ro_formats():
    assert parse_int_ro("13.000.000") == 13000000
    assert parse_int_ro("x482.708") == 482708
    assert parse_int_ro("(x7.825)") == 7825
    assert parse_int_ro("12 345") == 12345
    assert parse_int_ro("12\u202f345") == 12345
    assert parse_int_ro(None) is None
