import datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl

import analyze.main as analyze_main
from analyze.data import SingleDeviceData, SingleFileData
from importer.config_model import AnalyzedFiles

UTC = datetime.timezone.utc


def _file(name: str, values: list[tuple[int, int]]) -> SingleFileData:
    timestamps = [datetime.datetime.fromtimestamp(timestamp, tz=UTC) for timestamp, _ in values]
    df = pl.DataFrame(
        {
            "timestamp": [timestamp for timestamp, _ in values],
            "value": [value for _, value in values],
            "device": ["dev"] * len(values),
            "file": [name] * len(values),
        }
    )
    return SingleFileData(
        device="dev",
        file=Path(name),
        df=df,
        first_timestamp=timestamps[0],
        last_timestamp=timestamps[-1],
    )


def _device(files: list[SingleFileData]) -> SingleDeviceData:
    timestamps = sorted({timestamp for file in files for timestamp in file.df["timestamp"].to_list()})
    return SingleDeviceData(
        device="dev",
        df=pl.DataFrame(
            {"timestamp": [datetime.datetime.fromtimestamp(timestamp, tz=UTC) for timestamp in timestamps]}
        ),
        file_data=files,
    )


def test_find_redundant_files_without_files():
    assert not analyze_main.find_redundant_files(_device([]))


def test_find_redundant_files_single_file():
    file = _file("only.csv", [(0, 1)])

    assert not analyze_main.find_redundant_files(_device([file]))


def test_find_redundant_files_identical_files_keep_one_deterministically():
    first = _file("a.csv", [(0, 1), (60, 2)])
    second = _file("b.csv", [(0, 1), (60, 2)])

    redundant_files = analyze_main.find_redundant_files(_device([second, first]))
    assert redundant_files == [analyze_main.RedundantFile(second, [first])]


def test_find_redundant_files_rows_covered_by_multiple_files():
    first = _file("first.csv", [(0, 1)])
    second = _file("second.csv", [(60, 2), (120, 3)])
    combined_copy = _file("combined.csv", [(0, 1), (60, 2), (120, 3)])

    redundant_files = analyze_main.find_redundant_files(_device([first, second, combined_copy]))
    assert redundant_files == [analyze_main.RedundantFile(combined_copy, [first, second])]


def test_find_redundant_files_preserves_partial_overlap():
    first = _file("first.csv", [(0, 1), (60, 2)])
    second = _file("second.csv", [(60, 2), (120, 3)])

    assert not analyze_main.find_redundant_files(_device([first, second]))


def test_find_redundant_files_preserves_non_overlapping_files():
    first = _file("first.csv", [(0, 1)])
    second = _file("second.csv", [(120, 2)])

    assert not analyze_main.find_redundant_files(_device([first, second]))


def test_find_redundant_files_preserves_conflicting_values_at_same_timestamp():
    first = _file("first.csv", [(0, 1)])
    second = _file("second.csv", [(0, 2)])

    assert not analyze_main.find_redundant_files(_device([first, second]))


def test_report_device_data_prints_no_candidate_when_files_are_unique(capsys):
    analyze_main.report_device_data(_device([_file("only.csv", [(0, 1)])]))

    assert capsys.readouterr().out == "No redundant CSV files found for device 'dev'.\n"


def test_report_device_data_includes_file_and_covering_source_ranges(capsys):
    source = _file("a-source.csv", [(0, 1), (60, 2), (60 * 60 * 24 * 5 + 60, 3)])
    redundant = _file("z-redundant.csv", [(0, 1), (60, 2), (60 * 60 * 24 * 5 + 60, 3)])

    analyze_main.report_device_data(_device([source, redundant]))

    output = capsys.readouterr().out
    assert "  File: z-redundant.csv\n" in output
    expected_range = "1970-01-01 00:00 .. 1970-01-06 00:01 (5 days, 00:01)"
    assert f"  Data range: {expected_range}\n" in output
    assert "  Covered by:\n    - a-source.csv\n" in output
    assert f"      Data range: {expected_range}\n" in output


def test_report_formats_time_range_under_one_day(capsys):
    first = _file("a-short.csv", [(0, 1), (60 * 60 + 5 * 60, 2)])
    second = _file("b-short.csv", [(0, 1), (60 * 60 + 5 * 60, 2)])

    analyze_main.report_device_data(_device([first, second]))

    assert "(01:05)" in capsys.readouterr().out


def test_report_device_data_does_not_warn_for_minute_samples(capsys):
    analyze_main.report_device_data(_device([_file("only.csv", [(0, 1), (60, 2)])]))

    assert "WARNING" not in capsys.readouterr().out


def test_report_device_data_warns_about_missing_time_ranges(capsys):
    first = _file("first.csv", [(0, 1)])
    second = _file("second.csv", [(180, 2)])

    analyze_main.report_device_data(_device([first, second]))

    output = capsys.readouterr().out
    assert "WARNING: Device 'dev' has a data gap between" in output
    assert "1970-01-01 00:00 and 1970-01-01 00:03" in output
    assert "UTC" not in output
    assert "(0:03:00)" in output


def test_main_reports_redundant_csv_and_missing_data(tmp_path: Path, monkeypatch, capsys):
    rows = "timestamp,value\n60,1\n300,2\n"
    (tmp_path / "a.csv").write_text(rows, encoding="utf-8")
    (tmp_path / "b.csv").write_text(rows, encoding="utf-8")
    monkeypatch.setattr(analyze_main, "config", SimpleNamespace(files=[AnalyzedFiles(device="dev", dir=tmp_path)]))

    analyze_main.main()

    output = capsys.readouterr().out
    assert "Redundant CSV candidate for device 'dev':" in output
    assert f"  File: {tmp_path}/b.csv\n" in output
    assert "  Data range: 1970-01-01 00:01 .. 1970-01-01 00:05 (00:04)\n" in output
    assert f"    - {tmp_path}/a.csv\n" in output
    assert "      Data range: 1970-01-01 00:01 .. 1970-01-01 00:05 (00:04)\n" in output
    assert "WARNING: Device 'dev' has a data gap between" in output
