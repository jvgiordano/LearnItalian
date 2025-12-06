import json
import csv
import difflib
import sys
import os
from pathlib import Path
from collections import defaultdict

# --- CONFIGURATION ---
TARGET_STATE_FILE = Path("state_progress_comprehensive.json")
LEVELS = ["A1", "A2", "B1", "B2", "C1"]

# --- IMPORT OFFICIAL TOPICS ---
try:
    from make_italian_datasets import TOPICS_BY_LEVEL
except ImportError:
    print("❌ Error: Could not import 'make_italian_datasets.py'.")
    print("Please ensure this script is in the same folder as your generator.")
    sys.exit(1)

def normalize_topic(dirty_topic, level):
    """Matches a messy CSV topic to the official canonical list."""
    if not dirty_topic: return None
    dirty_topic = dirty_topic.strip()
    official_list = TOPICS_BY_LEVEL.get(level, [])
    
    if dirty_topic in official_list: return dirty_topic
    
    matches = difflib.get_close_matches(dirty_topic, official_list, n=1, cutoff=0.4)
    if matches: return matches[0]
    return None

def find_csv_file(level):
    """
    Searches for the CSV file in 'data/' folder OR current folder.
    Case-insensitive search.
    """
    search_filename = f"italian_{level}.csv".lower()
    
    # Places to look: 1. Inside 'data' folder, 2. Current folder
    search_dirs = [Path("data"), Path(".")]
    
    for folder in search_dirs:
        if not folder.exists():
            continue
            
        # List all files in this folder
        try:
            for file in folder.iterdir():
                if file.is_file() and file.name.lower() == search_filename:
                    return file
        except OSError:
            continue
            
    return None

def sync_state():
    print(f"🔄 SYNCING STATE FROM CSVS (ROBUST MODE)")
    print(f"   Target State File: {TARGET_STATE_FILE.absolute()}")
    print("==================================================")

    new_coverage = {}
    total_questions_found = 0
    
    for level in LEVELS:
        # Step 1: Find the file (ignoring case and folder issues)
        csv_path = find_csv_file(level)
        
        if not csv_path:
            print(f"   ⚠️  Skipping {level}: Could not find 'italian_{level}.csv' in 'data/' or current folder.")
            continue
            
        print(f"   Processing {level} (Found at: {csv_path})...", end="", flush=True)
        
        if level not in new_coverage:
            new_coverage[level] = defaultdict(int)
        
        row_count = 0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'topic' in row and row['topic'].strip():
                        clean_topic = normalize_topic(row['topic'], level)
                        if clean_topic:
                            new_coverage[level][clean_topic] += 1
                            row_count += 1
        except Exception as e:
            print(f"\n   ❌ Error reading {level}: {e}")
            continue

        print(f" Done! ({row_count} questions)")
        total_questions_found += row_count

    # Load existing state to preserve other keys
    current_state = {}
    if TARGET_STATE_FILE.exists():
        try:
            with open(TARGET_STATE_FILE, 'r', encoding='utf-8') as f:
                current_state = json.load(f)
        except:
            pass

    # Update coverage
    final_coverage = {lvl: dict(counts) for lvl, counts in new_coverage.items()}
    current_state["coverage"] = final_coverage
    
    if "seen_questions" not in current_state:
        current_state["seen_questions"] = {}

    # Save
    with open(TARGET_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(current_state, f, indent=2, ensure_ascii=False)

    print("==================================================")
    print(f"✅ SUCCESS! State updated.")
    print(f"   Total questions indexed: {total_questions_found}")

if __name__ == "__main__":
    sync_state()