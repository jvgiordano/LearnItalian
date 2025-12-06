import sqlite3
import csv
import os
import hashlib
import sys
import difflib
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# --- IMPORT OFFICIAL TOPICS ---
try:
    # Requires make_italian_datasets.py to be in the same directory
    from make_italian_datasets import TOPICS_BY_LEVEL
except ImportError:
    print("❌ Error: Could not import 'make_italian_datasets.py'.")
    print("Please ensure this script is in the same folder as your generator.")
    sys.exit(1)

# Configuration
DB_NAME = "italian_quiz.db"
DATA_DIR = Path("data")
LEVELS = ["A1", "A2", "B1", "B2", "C1"]
REPORT_FILE = "SETUP_REPORT.txt"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def normalize_topic(dirty_topic, level):
    """
    Maps a messy CSV topic to the official canonical list using fuzzy matching.
    """
    if not dirty_topic:
        return None

    dirty_topic = dirty_topic.strip()
    official_list = TOPICS_BY_LEVEL.get(level, [])
    
    # 1. Exact match
    if dirty_topic in official_list:
        return dirty_topic
        
    # 2. Fuzzy match
    matches = difflib.get_close_matches(dirty_topic, official_list, n=1, cutoff=0.4)
    if matches:
        return matches[0]
    
    return None # Returns None if no plausible official topic is found

def create_schema():
    print("1. Creating database structure (9 tables)...")
    
    # WARNING: This deletes the existing database file!
    try:
        os.remove(DB_NAME)
    except OSError:
        pass

    conn = get_db_connection()
    cursor = conn.cursor()

    # --- TABLE 1: QUESTIONS (Content) ---
    # Kept your new schema's columns: hash_id, complete_sentence, etc.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        complete_sentence TEXT NOT NULL,
        question_text TEXT NOT NULL,
        english_translation TEXT NOT NULL,
        hint TEXT,
        alternate_correct_responses TEXT,
        option_a TEXT NOT NULL,
        option_b TEXT NOT NULL,
        option_c TEXT NOT NULL,
        option_d TEXT NOT NULL,
        correct_option TEXT NOT NULL,
        cefr_level TEXT NOT NULL,
        topic TEXT NOT NULL,
        explanation TEXT,
        resource TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        hash_id TEXT UNIQUE
    )
    ''')
    
    # --- TABLE 2: ENHANCED PERFORMANCE (User Stats) ---
    # Kept your new schema's performance tracking columns
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS enhanced_performance (
        question_id INTEGER PRIMARY KEY,
        correct_count INTEGER DEFAULT 0,
        incorrect_count INTEGER DEFAULT 0,
        partial_correct_count INTEGER DEFAULT 0,
        freeform_correct_count INTEGER DEFAULT 0,
        
        last_seen TIMESTAMP, 
        
        last_answered_at TIMESTAMP,
        next_review_at TIMESTAMP,
        mastery_level REAL DEFAULT 0.0,
        history_string TEXT DEFAULT '',
        FOREIGN KEY (question_id) REFERENCES questions (id)
    )
    ''')
    
    # --- TABLE 3: TOPIC PERFORMANCE (CRITICAL FIX) ---
    # Copied from the old script to fix 'no such table' error
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS topic_performance (
        topic TEXT,
        cefr_level TEXT,
        correct_count INTEGER DEFAULT 0,
        incorrect_count INTEGER DEFAULT 0,
        last_updated TIMESTAMP,
        PRIMARY KEY (topic, cefr_level)
    )
    ''')

    # --- TABLE 4: USER PROGRESS (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_progress (
        id INTEGER PRIMARY KEY,
        estimated_level TEXT DEFAULT 'A1',
        total_questions_answered INTEGER DEFAULT 0,
        last_assessment TIMESTAMP,
        level_confidence REAL DEFAULT 0.5
    )
    ''')

    # --- TABLE 5: LEVEL STATS (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS level_stats (
        level TEXT PRIMARY KEY,
        history TEXT NOT NULL
    )
    ''')

    # --- TABLE 6: QUIZ HISTORY (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS quiz_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        score INTEGER NOT NULL,
        total_questions INTEGER NOT NULL,
        session_id TEXT
    )
    ''')

    # --- TABLE 7: DETAILED ANSWER HISTORY (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS answer_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_id INTEGER,
        user_answer TEXT,
        correct_answer TEXT,
        is_correct BOOLEAN,
        timestamp DATETIME,
        quiz_session_id TEXT,
        cefr_level TEXT NOT NULL,
        FOREIGN KEY (question_id) REFERENCES questions (id)
    )
    ''')

    # --- TABLE 8: DAILY STATS (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        total_coverage REAL DEFAULT 0,
        total_mastery REAL DEFAULT 0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # --- TABLE 9: QUESTION UPDATE LOG (CRITICAL FIX) ---
    # Copied from the old script
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS question_update_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_hash TEXT NOT NULL,
        old_question_id INTEGER,
        new_question_id INTEGER,
        update_type TEXT NOT NULL,
        timestamp DATETIME,
        notes TEXT
    )
    ''')
    
    # Create indices for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_level_topic ON questions(cefr_level, topic)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_perf_question_id ON enhanced_performance(question_id)')
    # Index from old script
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_question_hash ON questions(hash_id)')

    
    conn.commit()
    conn.close()
    print("Database structure created successfully (Questions + 8 support tables).\n")

def find_csv_file(level):
    """
    Searches for the CSV file by checking both capitalized and lowercase filenames,
    and also checks the current folder if the 'data' folder isn't found/used.
    """
    search_filename_cap = f"Italian_{level}.csv"
    search_filename_low = f"italian_{level}.csv"
    
    # Search paths: 1. data/ subfolder, 2. Current folder
    search_dirs = [Path("data"), Path(".")]
    
    for folder in search_dirs:
        if not folder.exists():
            continue
        
        # Check capitalized version first (Italian_A1.csv)
        path_cap = folder / search_filename_cap
        if path_cap.exists():
            return path_cap
            
        # Check lowercase version (italian_A1.csv)
        path_low = folder / search_filename_low
        if path_low.exists():
            return path_low
            
    return None

def populate_database():
    print("2. Populating with questions from CSV files...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_added = 0
    total_skipped_dupe = 0
    total_errors = 0
    topics_normalized_count = 0
    
    seen_hashes = set()
    
    for level in LEVELS:
        csv_path = find_csv_file(level)
        
        if not csv_path:
            print(f"Skipping {level}: CSV file not found.")
            continue
            
        print(f"Processing '{csv_path}'...")
        
        added_this_level = 0
        errors_this_level = 0
        dupes_this_level = 0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row_num, row in enumerate(reader, 1):
                    # 1. Basic Validation
                    required = ['question_text', 'correct_option', 'option_a', 'topic']
                    if any(not row.get(field) for field in required):
                        errors_this_level += 1
                        continue

                    # 2. Validate Correct Option
                    correct = row['correct_option'].strip().upper()
                    if correct not in ['A', 'B', 'C', 'D']:
                        errors_this_level += 1
                        continue

                    # 3. Generate Hash (Prevent Duplicates) - Using your new script's simpler hash
                    q_text = row['question_text'].strip()
                    content_hash = hashlib.md5(f"{level}:{q_text}".encode()).hexdigest()[:16]
                    
                    if content_hash in seen_hashes:
                        dupes_this_level += 1
                        continue
                    seen_hashes.add(content_hash)

                    # 4. NORMALIZE TOPIC
                    original_topic = row['topic'].strip()
                    clean_topic = normalize_topic(original_topic, level)
                    
                    if original_topic != clean_topic:
                        topics_normalized_count += 1

                    if not clean_topic: # Skip if the topic could not be normalized (unrecognized)
                        errors_this_level += 1
                        continue

                    # 5. Insert Question
                    try:
                        current_time = datetime.now().isoformat()
                        
                        cursor.execute('''
                            INSERT INTO questions (
                                complete_sentence, question_text, english_translation,
                                hint, alternate_correct_responses,
                                option_a, option_b, option_c, option_d,
                                correct_option, cefr_level, topic,
                                explanation, resource, hash_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row.get('complete_sentence', '').strip(),
                            q_text,
                            row.get('english_translation', '').strip(),
                            row.get('hint', '').strip(),
                            row.get('alternate_correct_responses', '').strip(),
                            row.get('option_a', '').strip(),
                            row.get('option_b', '').strip(),
                            row.get('option_c', '').strip(),
                            row.get('option_d', '').strip(),
                            correct,
                            level,
                            clean_topic,
                            row.get('explanation', '').strip(),
                            row.get('resource', '').strip(),
                            content_hash
                        ))
                        
                        # 6. Initialize Performance Record (Empty)
                        question_id = cursor.lastrowid
                        # Only insert question_id, all other columns use their default (0/NULL)
                        cursor.execute('''
                            INSERT INTO enhanced_performance (question_id) VALUES (?)
                        ''', (question_id,))
                        
                        # Initialize User Progress (if not done already - safe to ignore if running multiple times)
                        cursor.execute("INSERT OR IGNORE INTO user_progress (id) VALUES (1)")
                        
                        # Initialize Topic Performance (Optional, but safe to do here)
                        cursor.execute('''
                            INSERT OR IGNORE INTO topic_performance (topic, cefr_level) VALUES (?, ?)
                        ''', (clean_topic, level))
                        
                        # Log the creation
                        cursor.execute('''
                        INSERT INTO question_update_log 
                        (question_hash, new_question_id, update_type, timestamp, notes)
                        VALUES (?, ?, ?, ?, ?)
                        ''', (content_hash, question_id, 'created', 
                             current_time, f'Initial load from {csv_path} row {row_num}'))
                        
                        added_this_level += 1
                    except Exception as e:
                        print(f"Error inserting row {row_num}: {e}")
                        errors_this_level += 1
        
        except Exception as e:
             print(f"Fatal error during processing '{csv_path}': {e}")
             errors_this_level = 999 # Indicate major failure
             
        print(f"  Added {added_this_level} questions from '{csv_path}' ({errors_this_level} errors, {dupes_this_level} duplicates skipped)")
        
        total_added += added_this_level
        total_errors += errors_this_level
        total_skipped_dupe += dupes_this_level

    conn.commit()
    conn.close()
    
    print("\nDatabase population complete!")
    print(f"Total questions added: {total_added}")
    print(f"Topics auto-corrected: {topics_normalized_count}")

def verify_and_report():
    print("3. Verifying database and generating report...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    report_lines = []
    
    # Verify Performance Table Count
    cursor.execute("SELECT COUNT(*) as count FROM enhanced_performance")
    perf_count = cursor.fetchone()['count']
    print(f"Performance Records Initialized: {perf_count}")
    
    # Topic Consistency Check (Proof that normalization worked)
    print("\nTopic Coverage (Comparing against official list):")
    total_unique_db_topics = 0
    
    for level in LEVELS:
        official_topics = set(TOPICS_BY_LEVEL.get(level, []))
        cursor.execute("SELECT DISTINCT topic FROM questions WHERE cefr_level = ?", (level,))
        db_topics = set(row['topic'] for row in cursor.fetchall())
        
        total_unique_db_topics += len(db_topics)
        
        extra_topics = db_topics - official_topics
        status = "✓" if not extra_topics else "!"
        msg = f"  {status} {level}: {len(db_topics)} topics found (Expected: {len(official_topics)})"
        print(msg)
        report_lines.append(msg)
        
    print(f"\nTotal unique topics in DB: {total_unique_db_topics}")
    
    # Save Report
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\nReport generated: {REPORT_FILE}")
    conn.close()

if __name__ == "__main__":
    print("ITALIAN QUIZ - NEW DATABASE SETUP (FIXED SCHEMA)")
    print("============================================================")
    
    create_schema()
    populate_database()
    verify_and_report()
    
    print("============================================================")
    print("✅ SUCCESS! New database 'italian_quiz.db' is ready to use.")
    print("============================================================")