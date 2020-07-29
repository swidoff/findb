import argparse
import csv
from datetime import datetime

import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replace dates with integers and timestamps with epoch in a csv file")
    parser.add_argument("-d", type=int, nargs="*", help="The column numbers (0-based) of date columns")
    parser.add_argument("-t", type=int, nargs="*", help="The column numbers (0-based) of timestamp columns")

    ns = parser.parse_args()
    date_cols = ns.d or []
    ts_cols = ns.t or []

    if not date_cols and not ts_cols:
        parser.print_usage()
        exit(-1)

    reader = csv.reader(sys.stdin)
    writer = csv.writer(sys.stdout)
    for row_num, row in enumerate(reader, 1):
        i = 0
        try:
            for i in date_cols:
                row[i] = datetime.fromisoformat(row[i]).strftime("%Y%m%d")

            for i in ts_cols:
                row[i] = round(datetime.fromisoformat(row[i]).timestamp())

        except ValueError as e:
            print(f"Line {row_num}, column {i + 1}: {str(e)}.", file=sys.stderr)
            exit(-1)

        writer.writerow(row)

exit(0)
