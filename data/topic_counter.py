import csv
import sys
from collections import Counter
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
TOPIC_COL   = "topic"          # column name for the topic
MIN_WARNING = 10               # flag topics with fewer questions than this
# ────────────────────────────────────────────────────────────────────────────


def analyse(filepath: str) -> None:
    path = Path(filepath)
    if not path.exists():
        print(f"Error: file not found → {filepath}")
        sys.exit(1)

    counts: Counter = Counter()
    total = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if TOPIC_COL not in (reader.fieldnames or []):
            print(f"Error: column '{TOPIC_COL}' not found in {path.name}")
            print(f"  Available columns: {reader.fieldnames}")
            sys.exit(1)

        for row in reader:
            topic = row[TOPIC_COL].strip()
            counts[topic] += 1
            total += 1

    # ── Print results ────────────────────────────────────────────────────────
    col_w = max(len(t) for t in counts) + 2  # dynamic column width
    sep   = "-" * (col_w + 20)

    print(f"\n{'='*60}")
    print(f"  File   : {path.name}")
    print(f"  Topics : {len(counts)}")
    print(f"  Total  : {total} questions")
    print(f"{'='*60}\n")

    print(f"{'Topic':<{col_w}} {'Questions':>9}  {'% of file':>9}  Flag")
    print(sep)

    for topic, count in sorted(counts.items(), key=lambda x: x[1]):
        pct  = count / total * 100
        flag = "⚠️  LOW" if count < MIN_WARNING else ""
        print(f"{topic:<{col_w}} {count:>9}  {pct:>8.1f}%  {flag}")

    print(sep)
    print(f"{'TOTAL':<{col_w}} {total:>9}\n")

    # ── Summary of low-count topics ──────────────────────────────────────────
    low = {t: c for t, c in counts.items() if c < MIN_WARNING}
    if low:
        print(f"⚠️  {len(low)} topic(s) have fewer than {MIN_WARNING} questions:")
        for t, c in sorted(low.items(), key=lambda x: x[1]):
            print(f"   • {t}: {c}")
    else:
        print(f"✅  All topics have {MIN_WARNING}+ questions.")

    # ── File-size sanity check ───────────────────────────────────────────────
    print()
    if total < 1400:
        print(f"⚠️  File has only {total} questions — below the 1,400 minimum.")
    elif total > 2600:
        print(f"⚠️  File has {total} questions — above the 2,600 maximum.")
    else:
        print(f"✅  File size is within the expected range (1,400–2,600).")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python topic_counter.py <path/to/file.csv>")
        print("       python topic_counter.py *.csv          (multiple files)")
        sys.exit(1)

    for arg in sys.argv[1:]:
        analyse(arg)
