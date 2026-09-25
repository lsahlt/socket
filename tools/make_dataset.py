#!/usr/bin/env python3
"""
Build data/details-<YYYY>.csv from the NCEI Storm Events bulk archive.

The project spec wants a 14-field CSV per year. NCEI publishes the same data
with ~50 columns, so this script downloads the year's "details" file and keeps
only the 14 fields the spec lists, in the spec's order.

    python3 tools/make_dataset.py 1950
    python3 tools/make_dataset.py 1950 --from ~/Downloads/StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz

Use --from when you already have the .gz, or when you are on a host without
outbound internet access.

If your instructor posts a ready-made details-1950.csv, use that instead --
this script is a fallback so you are not blocked.
"""

import argparse
import csv
import gzip
import io
import os
import re
import sys
import urllib.request

INDEX = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

# The 14 fields from section 1.2.1 of the project spec, in order, paired with
# the column name NCEI uses for each.
FIELDS = [
    ("event_id",          "EVENT_ID"),
    ("state",             "STATE"),
    ("year",              "YEAR"),
    ("month_name",        "MONTH_NAME"),
    ("event_type",        "EVENT_TYPE"),
    ("cz_type",           "CZ_TYPE"),
    ("cz_name",           "CZ_NAME"),
    ("injuries_direct",   "INJURIES_DIRECT"),
    ("injuries_indirect", "INJURIES_INDIRECT"),
    ("deaths_direct",     "DEATHS_DIRECT"),
    ("deaths_indirect",   "DEATHS_INDIRECT"),
    ("damage_property",   "DAMAGE_PROPERTY"),
    ("damage_crops",      "DAMAGE_CROPS"),
    ("tor_f_scale",       "TOR_F_SCALE"),
]


def find_remote_url(year):
    """The c-date suffix changes per year, so look it up in the index."""
    sys.stderr.write("listing {}\n".format(INDEX))
    with urllib.request.urlopen(INDEX, timeout=60) as response:
        html = response.read().decode("utf-8", "replace")
    pattern = r"StormEvents_details-ftp_v1\.0_d{}_c\d+\.csv\.gz".format(year)
    matches = sorted(set(re.findall(pattern, html)))
    if not matches:
        raise SystemExit("no details file listed for {}".format(year))
    return INDEX + matches[-1]  # newest c-date


def read_source(year, local_path):
    if local_path:
        sys.stderr.write("reading {}\n".format(local_path))
        with gzip.open(local_path, "rt", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    url = find_remote_url(year)
    sys.stderr.write("downloading {}\n".format(url))
    with urllib.request.urlopen(url, timeout=120) as response:
        raw = response.read()
    return gzip.decompress(raw).decode("utf-8", "replace")


def convert(text, out_path):
    reader = csv.DictReader(io.StringIO(text))
    missing = [ncei for _, ncei in FIELDS if ncei not in (reader.fieldnames or [])]
    if missing:
        raise SystemExit("source is missing expected columns: " + ", ".join(missing))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([spec for spec, _ in FIELDS])
        for row in reader:
            if not (row.get("EVENT_ID") or "").strip().isdigit():
                continue
            writer.writerow([(row.get(ncei) or "").strip() for _, ncei in FIELDS])
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("year", type=int, help="dataset year, 1950-2019")
    parser.add_argument("--from", dest="local", help="path to an already-downloaded .csv.gz")
    parser.add_argument("--out", help="output path (default data/details-<year>.csv)")
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = args.out or os.path.join(repo_root, "data",
                                        "details-{}.csv".format(args.year))

    text = read_source(args.year, args.local)
    count = convert(text, out_path)

    print("wrote {} ({} records, l={})".format(out_path, count, count))
    print("hash table size s will be the first prime greater than {}".format(2 * count))


if __name__ == "__main__":
    main()
