"""Unit tests for safe_parse_records() — 6 input shapes, all must return list or [].

Run: python3 test_safe_parse_records.py
"""

from insider_engine import safe_parse_records


def test_list_of_dicts():
    result = safe_parse_records([{"a": 1}, {"b": 2}])
    assert result == [{"a": 1}, {"b": 2}], f"expected list of dicts, got {result}"


def test_dict_with_filings_key():
    result = safe_parse_records({"filings": [{"date": "2026-07-01"}, {"date": "2026-06-01"}]})
    assert result == [{"date": "2026-07-01"}, {"date": "2026-06-01"}], \
        f"expected extracted filings list, got {result}"


def test_dict_with_no_known_key():
    result = safe_parse_records({"error": "rate limited"})
    assert result == [], f"expected [] for error envelope, got {result}"


def test_bare_string():
    result = safe_parse_records("rate limited by EDGAR")
    assert result == [], f"expected [] for bare string, got {result}"


def test_none():
    result = safe_parse_records(None)
    assert result == [], f"expected [] for None, got {result}"


def test_empty_body():
    result = safe_parse_records([])
    assert result == [], f"expected [] for empty list, got {result}"


def test_list_mixed_drops_nondict():
    result = safe_parse_records([{"a": 1}, "stray_string", None, {"b": 2}])
    assert result == [{"a": 1}, {"b": 2}], \
        f"expected non-dict items filtered out, got {result}"


def test_results_key():
    result = safe_parse_records({"results": [{"ticker": "AAPL"}]})
    assert result == [{"ticker": "AAPL"}], \
        f"expected results key extracted, got {result}"


if __name__ == "__main__":
    tests = [
        test_list_of_dicts,
        test_dict_with_filings_key,
        test_dict_with_no_known_key,
        test_bare_string,
        test_none,
        test_empty_body,
        test_list_mixed_drops_nondict,
        test_results_key,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} passed")
    if failed:
        raise SystemExit(1)
