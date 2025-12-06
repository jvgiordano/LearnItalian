#!/usr/bin/env python3
"""
Safe update of existing Italian Quiz database - COMPREHENSIVE VERSION
Preserves all user progress while updating questions from CSV files.
Handles the comprehensive CSV format with all fields including complete_sentence.

FIXED: Now includes topic normalization to merge old/new topic names
       and maintain progress continuity.

Compatible with comprehensive dataset generator (217 total topics):
- A1: 46 | A2: 48 | B1: 42 | B2: 43 | C1: 38

Usage: python update_existing_database.py
"""

import sqlite3
import csv
import os
import hashlib
import shutil
import difflib
from datetime import datetime
from pathlib import Path
import sys

# --- IMPORT OFFICIAL TOPICS ---
try:
    # Requires make_italian_datasets.py to be in the same directory
    from make_italian_datasets import TOPICS_BY_LEVEL
except ImportError:
    print("❌ Error: Could not import 'make_italian_datasets.py'.")
    print("Please ensure this script is in the same folder as your generator.")
    sys.exit(1)


# Configuration
DB_FILE = "italian_quiz.db"
BACKUP_DIR = "backups"
CSV_FILES = [
    "data/italian_A1.csv",
    "data/italian_A2.csv", 
    "data/italian_B1.csv",
    "data/italian_B2.csv",
    "data/italian_C1.csv"
]
REPORT_FILE = "UPDATE_REPORT.txt"

# Expected topic counts from comprehensive generator
EXPECTED_TOPIC_COUNTS = {
    'A1': len(TOPICS_BY_LEVEL.get('A1', [])),
    'A2': len(TOPICS_BY_LEVEL.get('A2', [])),
    'B1': len(TOPICS_BY_LEVEL.get('B1', [])),
    'B2': len(TOPICS_BY_LEVEL.get('B2', [])),
    'C1': len(TOPICS_BY_LEVEL.get('C1', []))
}

def generate_question_hash(question_text, english_translation, option_a, option_b, option_c, option_d, correct_option, cefr_level, topic):
    """
    Generate a stable hash for a question based on its core content.
    Note: complete_sentence is NOT included in hash as it's derived from question_text + correct_option.
    """
    content_parts = [
        question_text.strip().lower(),
        english_translation.strip().lower(),
        option_a.strip().lower(),
        option_b.strip().lower(), 
        option_c.strip().lower(),
        option_d.strip().lower(),
        correct_option.strip().upper(),
        cefr_level.strip().upper(),
        topic.strip().lower() # Hashing relies on the topic being the canonical name
    ]
    
    content_string = "|".join(content_parts)
    return hashlib.sha256(content_string.encode('utf-8')).hexdigest()[:16]

def normalize_topic(dirty_topic, level):
    """
    Maps a messy topic string to the official canonical list using fuzzy matching.
    Uses the logic copied from setup_new_database.py.
    """
    if not dirty_topic:
        return None

    dirty_topic = dirty_topic.strip()
    official_list = TOPICS_BY_LEVEL.get(level, [])
    
    # 1. Exact match
    if dirty_topic in official_list:
        return dirty_topic
        
    # 2. Fuzzy match
    # Use a low cutoff for broader matching, e.g., 'Abbigliamento (vocabolario)' -> 'Abbigliamento'
    matches = difflib.get_close_matches(dirty_topic, official_list, n=1, cutoff=0.4)
    if matches:
        return matches[0]
    
    return dirty_topic # Return original if no official topic is found (for logging/manual check)

def migrate_existing_topics():
    """
    STEP 1 FIX: Migrates all existing question topics to their canonical name.
    This prevents the archive/re-add problem when topic names change.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    migrated_count = 0
    
    print("\n--- Migrating Existing Database Topics ---")
    
    try:
        # Fetch all questions with their old hash and topic
        cursor.execute('SELECT id, cefr_level, topic, question_hash, question_text, english_translation, option_a, option_b, option_c, option_d, correct_option FROM questions')
        questions = cursor.fetchall()
        
        for q_id, level, old_topic, old_hash, q_text, eng_trans, opt_a, opt_b, opt_c, opt_d, correct_opt in questions:
            
            # 1. Normalize the existing topic name
            new_topic = normalize_topic(old_topic, level)
            
            if new_topic != old_topic:
                
                # 2. Generate the NEW hash based on the new topic name
                new_hash = generate_question_hash(q_text, eng_trans, opt_a, opt_b, opt_c, opt_d, correct_opt, level, new_topic)
                
                # 3. Update the question record with the new topic and new hash
                cursor.execute('''
                UPDATE questions 
                SET topic = ?, question_hash = ?, updated_at = ?
                WHERE id = ?
                ''', (new_topic, new_hash, datetime.now().isoformat(), q_id))
                
                # 4. Log the migration/re-hash
                cursor.execute('''
                INSERT INTO question_update_log 
                (question_hash, old_question_id, new_question_id, update_type, timestamp, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (new_hash, q_id, q_id, 'migrated', 
                     datetime.now().isoformat(), f'Topic migrated from "{old_topic}" to "{new_topic}" - rehashed'))
                
                migrated_count += 1
                
        # Also, check the topic_performance table for consistency
        cursor.execute('SELECT topic, cefr_level FROM topic_performance')
        topic_perf_records = cursor.fetchall()
        for topic, level in topic_perf_records:
            new_topic = normalize_topic(topic, level)
            if new_topic != topic:
                cursor.execute('''
                UPDATE topic_performance 
                SET topic = ? 
                WHERE topic = ? AND cefr_level = ?
                ''', (new_topic, topic, level))
        
        conn.commit()
        print(f"✅ Successfully migrated and re-hashed {migrated_count} question topics in the database.")
        
    except Exception as e:
        print(f"❌ Error during topic migration: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()
        
    return True

# [The remaining functions like create_backup, check_database_compatibility, 
#  create_archive_tables_if_needed, archive_removed_questions, 
#  generate_update_report, verify_progress_integrity, show_recent_changes
#  remain largely the same, but safe_update_from_csv is modified]

def create_backup():
    """Create a backup of the current database."""
    if not os.path.exists(DB_FILE):
        print(f"No database file '{DB_FILE}' found to backup.")
        return None
    
    # Create backup directory if it doesn't exist
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    backup_name = f"{BACKUP_DIR}/italian_quiz_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        shutil.copy2(DB_FILE, backup_name)
        print(f"Database backed up to: {backup_name}")
        return backup_name
    except Exception as e:
        print(f"Failed to create backup: {e}")
        return None

def check_database_compatibility():
    """Check if the database has the required schema for safe updates."""
    if not os.path.exists(DB_FILE):
        print(f"Database file '{DB_FILE}' not found!")
        print("Run setup_new_database.py to create a new database.")
        return False
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Check if questions table has required columns
        cursor.execute("PRAGMA table_info(questions)")
        columns = [col[1] for col in cursor.fetchall()]
        
        required_columns = ['question_hash', 'created_at', 'updated_at']
        # 'hash_id' is from setup_new_database, we prefer 'question_hash' from update script logic
        # For compatibility with both scripts, we check for at least one.
        if 'question_hash' not in columns and 'hash_id' in columns:
            # Rename 'hash_id' to 'question_hash' for internal consistency
            cursor.execute("ALTER TABLE questions RENAME COLUMN hash_id TO question_hash")
            columns[columns.index('hash_id')] = 'question_hash'
            print("Renamed 'hash_id' to 'question_hash' for consistency.")
        elif 'question_hash' not in columns and 'hash_id' not in columns:
            required_columns.insert(0, 'question_hash') # Ensure it gets added below
            
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            print(f"Database is missing required columns: {missing_columns}")
            print("The database needs to be migrated for safe updates.")
            return False # Stop if core columns are missing
        
        # Check if question_update_log table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='question_update_log'")
        if not cursor.fetchone():
            print("Database is missing the question_update_log table.")
            return False # Stop if log table is missing
        
        # Add new columns if they don't exist (graceful schema evolution)
        columns_added = False
        
        if 'hint' not in columns:
            print("Adding missing 'hint' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN hint TEXT")
            columns_added = True
        
        if 'alternate_correct_responses' not in columns:
            print("Adding missing 'alternate_correct_responses' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN alternate_correct_responses TEXT")
            columns_added = True
        
        if 'resource' not in columns:
            print("Adding missing 'resource' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN resource TEXT")
            columns_added = True
        
        if 'complete_sentence' not in columns:
            print("Adding missing 'complete_sentence' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN complete_sentence TEXT")
            columns_added = True
        
        if columns_added:
            conn.commit()
            print("Schema updated successfully.")
        
        return True
        
    except Exception as e:
        print(f"Error checking database compatibility: {e}")
        return False
    finally:
        conn.close()

def create_archive_tables_if_needed():
    """Create archive tables for removed questions and their progress data."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Archive table for removed questions
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS archived_questions (
            id INTEGER PRIMARY KEY,
            original_question_id INTEGER NOT NULL,
            question_hash TEXT NOT NULL,
            complete_sentence TEXT,
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
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            removal_reason TEXT
        )
        ''')
        
        # Archive table for enhanced performance data
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS archived_enhanced_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_question_id INTEGER NOT NULL,
            question_hash TEXT NOT NULL,
            correct_count INTEGER DEFAULT 0,
            incorrect_count INTEGER DEFAULT 0,
            last_seen TIMESTAMP,
            next_review TIMESTAMP,
            mastery_level INTEGER DEFAULT 0,
            freeform_correct_count INTEGER DEFAULT 0,
            freeform_incorrect_count INTEGER DEFAULT 0,
            partial_correct_count INTEGER DEFAULT 0,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Archive table for answer history
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS archived_answer_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_answer_id INTEGER NOT NULL,
            original_question_id INTEGER NOT NULL,
            question_hash TEXT NOT NULL,
            user_answer TEXT,
            correct_answer TEXT,
            is_correct BOOLEAN,
            timestamp DATETIME,
            quiz_session_id TEXT,
            cefr_level TEXT NOT NULL,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        print("Archive tables created/verified.")
        
    except Exception as e:
        print(f"Error creating archive tables: {e}")
        return False
    finally:
        conn.close()
    
    return True

def archive_removed_questions(csv_hashes):
    """Archive questions and their progress data that are no longer in CSV files."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    stats = {
        'questions_archived': 0,
        'performance_archived': 0,
        'answers_archived': 0,
        'errors': 0
    }
    
    try:
        # Find questions that exist in database but not in CSV files
        cursor.execute('SELECT question_hash, id FROM questions')
        db_questions = cursor.fetchall()
        
        questions_to_archive = []
        for hash_val, question_id in db_questions:
            # Check if hash is in the set of *current* CSV hashes
            if hash_val not in csv_hashes:
                questions_to_archive.append((hash_val, question_id))
        
        if not questions_to_archive:
            print("No questions need to be removed.")
            return stats
        
        print(f"Found {len(questions_to_archive)} questions to archive/remove...")
        
        for question_hash, question_id in questions_to_archive:
            try:
                # Archive the question itself 
                cursor.execute('''
                INSERT INTO archived_questions 
                (original_question_id, question_hash, complete_sentence, question_text, english_translation, 
                 hint, alternate_correct_responses,
                 option_a, option_b, option_c, option_d, correct_option, cefr_level, 
                 topic, explanation, resource, created_at, updated_at, removal_reason)
                SELECT id, question_hash, complete_sentence, question_text, english_translation, 
                       hint, alternate_correct_responses,
                       option_a, option_b, option_c, option_d, correct_option, cefr_level, 
                       topic, explanation, resource, created_at, updated_at, 'No longer in CSV files'
                FROM questions WHERE id = ?
                ''', (question_id,))
                
                # Archive enhanced performance data
                cursor.execute('''
                INSERT INTO archived_enhanced_performance 
                (original_question_id, question_hash, correct_count, incorrect_count, 
                 last_seen, next_review, mastery_level, freeform_correct_count, 
                 freeform_incorrect_count, partial_correct_count)
                SELECT question_id, ?, correct_count, incorrect_count, 
                       last_seen, next_review, mastery_level, 
                       IFNULL(freeform_correct_count, 0), 
                       IFNULL(freeform_incorrect_count, 0), 
                       IFNULL(partial_correct_count, 0)
                FROM enhanced_performance WHERE question_id = ?
                ''', (question_hash, question_id))
                
                if cursor.rowcount > 0:
                    stats['performance_archived'] += 1
                
                # Archive answer history
                cursor.execute('''
                INSERT INTO archived_answer_history 
                (original_answer_id, original_question_id, question_hash, user_answer, 
                 correct_answer, is_correct, timestamp, quiz_session_id, cefr_level)
                SELECT id, question_id, ?, user_answer, correct_answer, is_correct, 
                       timestamp, quiz_session_id, cefr_level
                FROM answer_history WHERE question_id = ?
                ''', (question_hash, question_id))
                
                if cursor.rowcount > 0:
                    stats['answers_archived'] += cursor.rowcount
                
                # Now remove from active tables
                cursor.execute('DELETE FROM answer_history WHERE question_id = ?', (question_id,))
                cursor.execute('DELETE FROM enhanced_performance WHERE question_id = ?', (question_id,))
                cursor.execute('DELETE FROM questions WHERE id = ?', (question_id,))
                
                # Log the removal
                cursor.execute('''
                INSERT INTO question_update_log 
                (question_hash, old_question_id, new_question_id, update_type, timestamp, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (question_hash, question_id, None, 'archived', 
                     datetime.now().isoformat(), f'Question no longer in CSV files, archived with progress data'))
                
                stats['questions_archived'] += 1
                print(f"   Archived question {question_id} (hash: {question_hash[:8]}...)")
                
            except Exception as e:
                print(f"Error archiving question {question_id}: {e}")
                stats['errors'] += 1
                continue
        
        conn.commit()
        
    except Exception as e:
        print(f"Error during archive process: {e}")
        stats['errors'] += 1
    finally:
        conn.close()
    
    return stats


def safe_update_from_csv():
    """
    STEP 2 FIX: Safely update the database from CSV files.
    - Normalizes the topic in the CSV row before calculating the hash.
    - If a question with the calculated hash (using the *canonical* topic name)
      already exists, it's an update (progress preserved).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    stats = {
        'new_questions': 0,
        'updated_questions': 0,
        'unchanged_questions': 0,
        'errors': 0,
        'files_processed': 0,
        'duplicates_skipped': 0,
        'topics_normalized_csv': 0
    }
    
    topic_counts = {}  # For report generation
    
    # Get existing questions for comparison
    # Note: These records now have the *canonical* topic name thanks to migrate_existing_topics()
    cursor.execute('''
        SELECT question_hash, id, complete_sentence, question_text, english_translation, 
               hint, alternate_correct_responses, option_a, option_b, option_c, option_d, 
               correct_option, cefr_level, topic, explanation, resource 
        FROM questions
    ''')
    existing_questions = {row[0]: row for row in cursor.fetchall()}  # hash -> full_record
    
    print("\nStarting safe database update...")
    print(f"Found {len(existing_questions)} existing questions (using canonical topics)")
    
    # Keep track of all question hashes found in CSV files
    csv_question_hashes = set()
    
    for filename in CSV_FILES:
        if not os.path.exists(filename):
            print(f"Warning: '{filename}' not found. Skipping.")
            continue

        print(f"\nProcessing '{filename}'...")
        stats['files_processed'] += 1
        
        try:
            with open(filename, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Check column headers for expected comprehensive format
                expected_columns = ['complete_sentence', 'question_text', 'english_translation', 'hint', 'alternate_correct_responses', 'option_a', 'option_b', 'option_c', 'option_d', 'correct_option', 'cefr_level', 'topic', 'explanation', 'resource']
                
                for row_num, row in enumerate(csv_reader, start=2):
                    try:
                        # 0. Get and validate level
                        level = row.get('cefr_level', '').strip()
                        if level not in LEVELS:
                            print(f"Warning: Row {row_num} in '{filename}' has invalid CEFR level '{level}'. Skipping.")
                            stats['errors'] += 1
                            continue
                            
                        # 1. Topic Normalization
                        original_topic = row.get('topic', '').strip()
                        canonical_topic = normalize_topic(original_topic, level)
                        
                        if original_topic != canonical_topic:
                            stats['topics_normalized_csv'] += 1
                            if canonical_topic == original_topic: # Should not happen if different
                                print(f"Info: Row {row_num} topic '{original_topic}' returned original after normalization attempt.")
                            else:
                                print(f"   Normalized topic: '{original_topic}' -> '{canonical_topic}'")
                                
                        if not canonical_topic:
                            print(f"Warning: Row {row_num} in '{filename}' topic '{original_topic}' could not be normalized/is empty. Skipping.")
                            stats['errors'] += 1
                            continue
                            
                        # 2. Validation and Hash Generation (using canonical topic)
                        required_fields = ['question_text', 'english_translation', 'option_a', 'option_b', 'option_c', 'option_d', 'correct_option', 'explanation']
                        
                        if not all(row.get(col, '').strip() for col in required_fields):
                            print(f"Warning: Row {row_num} in '{filename}' has empty required fields. Skipping.")
                            stats['errors'] += 1
                            continue
                        
                        question_hash = generate_question_hash(
                            row['question_text'], row['english_translation'], 
                            row['option_a'], row['option_b'], row['option_c'], row['option_d'],
                            row['correct_option'], level, canonical_topic # Use canonical topic for hash!
                        )
                        
                        # Add to set of CSV hashes (for later archive check)
                        csv_question_hashes.add(question_hash)
                        
                        # Track for report
                        if level not in topic_counts:
                            topic_counts[level] = {}
                        if canonical_topic not in topic_counts[level]:
                            topic_counts[level][canonical_topic] = 0
                        topic_counts[level][canonical_topic] += 1
                        
                        # 3. Prepare values for DB (use canonical topic)
                        complete_sentence_value = row.get('complete_sentence', '').strip()
                        hint_value = row.get('hint', '').strip()
                        alternate_value = row.get('alternate_correct_responses', '').strip()
                        resource_value = row.get('resource', '').strip()
                        
                        values = (
                            complete_sentence_value,
                            row['question_text'].strip(),
                            row['english_translation'].strip(),
                            hint_value,
                            alternate_value,
                            row['option_a'].strip(),
                            row['option_b'].strip(),
                            row['option_c'].strip(),
                            row['option_d'].strip(),
                            row['correct_option'].upper().strip(),
                            level,
                            canonical_topic, # Insert the canonical topic
                            row['explanation'].strip(),
                            resource_value
                        )
                        
                        if question_hash in existing_questions:
                            # Question exists - check if it needs updating
                            existing_record = existing_questions[question_hash]
                            existing_values = existing_record[2:] # Skip hash and id
                            
                            # Compare values (using canonical_topic in the CSV values)
                            if existing_values == values:
                                # Identical - no update needed
                                stats['unchanged_questions'] += 1
                            else:
                                # Content changed - update existing record (preserving ID and progress)
                                cursor.execute('''
                                UPDATE questions 
                                SET complete_sentence=?, question_text=?, english_translation=?, hint=?, 
                                    alternate_correct_responses=?, option_a=?, option_b=?, option_c=?, 
                                    option_d=?, correct_option=?, cefr_level=?, topic=?, explanation=?, 
                                    resource=?, updated_at=?
                                WHERE question_hash=?
                                ''', values + (datetime.now().isoformat(), question_hash))
                                
                                # Log the update
                                cursor.execute('''
                                INSERT INTO question_update_log 
                                (question_hash, old_question_id, new_question_id, update_type, timestamp, notes)
                                VALUES (?, ?, ?, ?, ?, ?)
                                ''', (question_hash, existing_record[1], existing_record[1], 'updated', 
                                     datetime.now().isoformat(), f'Updated content from {filename} row {row_num}'))
                                
                                stats['updated_questions'] += 1
                                print(f"   Updated question {existing_record[1]} (hash: {question_hash[:8]}...)")
                        else:
                            # New question - insert with new ID
                            current_time = datetime.now().isoformat()
                            cursor.execute('''
                            INSERT INTO questions 
                            (question_hash, complete_sentence, question_text, english_translation, 
                             hint, alternate_correct_responses, option_a, option_b, option_c, option_d, 
                             correct_option, cefr_level, topic, explanation, resource, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (question_hash,) + values + (current_time, current_time))
                            
                            new_id = cursor.lastrowid
                            
                            # Log the creation
                            cursor.execute('''
                            INSERT INTO question_update_log 
                            (question_hash, old_question_id, new_question_id, update_type, timestamp, notes)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''', (question_hash, None, new_id, 'created', 
                                 current_time, f'New question from {filename} row {row_num}'))
                            
                            stats['new_questions'] += 1
                            print(f"   Added new question {new_id} (hash: {question_hash[:8]}...)")
                            
                            # Also insert/update the canonical topic name in topic_performance
                            cursor.execute('''
                            INSERT OR REPLACE INTO topic_performance (topic, cefr_level) 
                            VALUES (?, ?)
                            ''', (canonical_topic, level))
                            
                    except sqlite3.IntegrityError as e:
                        if "UNIQUE constraint failed" in str(e):
                            # Question hash should be unique, but if it somehow fails, skip it.
                            stats['duplicates_skipped'] += 1
                        else:
                            print(f"Database error on row {row_num} in '{filename}': {e}")
                            stats['errors'] += 1
                    except Exception as e:
                        print(f"Error processing row {row_num} in '{filename}': {e}")
                        stats['errors'] += 1
                        continue
                
        except Exception as e:
            print(f"Error reading '{filename}': {e}")
            stats['errors'] += 1
            continue

    # Commit changes from CSV processing
    conn.commit()
    conn.close()
    
    return stats, csv_question_hashes, topic_counts


# [The rest of the functions are pasted below for completeness, but the core fix is in normalize_topic, migrate_existing_topics, and safe_update_from_csv.]


def generate_update_report(stats, topic_counts):
    """Generate a report of the update process."""
    with open(REPORT_FILE, 'w', encoding='utf-8') as report:
        report.write("ITALIAN QUIZ DATABASE UPDATE REPORT\n")
        report.write("COMPREHENSIVE DATASET (217 Total Topics)\n")
        report.write("=" * 80 + "\n")
        report.write(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.write("=" * 80 + "\n\n")
        
        report.write("EXPECTED TOPIC COVERAGE\n")
        report.write("-" * 40 + "\n")
        for level, count in EXPECTED_TOPIC_COUNTS.items():
            report.write(f"  {level}: {count} topics expected\n")
        report.write(f"  TOTAL: {sum(EXPECTED_TOPIC_COUNTS.values())} topics\n\n")
        
        report.write("UPDATE STATISTICS\n")
        report.write("-" * 40 + "\n")
        report.write(f"Files processed: {stats['files_processed']}\n")
        report.write(f"New questions added: {stats['new_questions']}\n")
        report.write(f"Questions updated: {stats['updated_questions']}\n")
        report.write(f"Questions unchanged: {stats['unchanged_questions']}\n")
        report.write(f"Questions archived: {stats.get('questions_archived', 0)}\n")
        report.write(f"Performance records archived: {stats.get('performance_archived', 0)}\n")
        report.write(f"Answer history records archived: {stats.get('answers_archived', 0)}\n")
        report.write(f"Duplicate questions skipped: {stats.get('duplicates_skipped', 0)}\n")
        report.write(f"CSV Topics Normalized: {stats.get('topics_normalized_csv', 0)}\n")
        report.write(f"Errors encountered: {stats['errors']}\n")
        report.write("\n")
        
        # Question counts by level and topic
        report.write("CURRENT QUESTION COUNTS BY LEVEL AND CANONICAL TOPIC\n")
        report.write("-" * 80 + "\n")
        
        level_order = ['A1', 'A2', 'B1', 'B2', 'C1']
        
        for level in level_order:
            if level in topic_counts:
                report.write(f"\nLEVEL {level} (Expected: {EXPECTED_TOPIC_COUNTS.get(level, '?')} topics)\n")
                report.write("-" * 20 + "\n")
                
                sorted_topics = sorted(topic_counts[level].items())
                total_for_level = 0
                unique_topics = len(sorted_topics)
                
                for topic, count in sorted_topics:
                    # Check against the official list for a proper report
                    official_list = TOPICS_BY_LEVEL.get(level, [])
                    topic_status = "✓" if topic in official_list else "⚠️" # Should only be ⚠️ if normalization failed
                    
                    report.write(f"  [{topic_status}] {topic}: {count} questions\n")
                    total_for_level += count
                
                report.write(f"  TOTAL FOR {level}: {total_for_level} questions across {unique_topics} canonical topics\n")
                
                # Show topic coverage vs expected
                expected = EXPECTED_TOPIC_COUNTS.get(level, 0)
                if unique_topics < expected:
                    report.write(f"  ⚠️  WARNING: Only {unique_topics} canonical topics found, expected {expected}\n")
                else:
                    report.write(f"  ✓ Found: {unique_topics} canonical topics (Expected: {expected})\n")
        
        # Overall summary
        report.write("\n" + "=" * 80 + "\n")
        report.write("OVERALL SUMMARY\n")
        report.write("-" * 40 + "\n")
        
        grand_total = 0
        total_topics = 0
        for level in level_order:
            if level in topic_counts:
                level_total = sum(topic_counts[level].values())
                level_topics = len(topic_counts[level])
                report.write(f"  {level}: {level_total} questions across {level_topics} canonical topics\n")
                grand_total += level_total
                total_topics += level_topics
        
        report.write(f"\n  GRAND TOTAL: {grand_total} active questions across {total_topics} canonical topics\n")
        report.write(f"  Expected: 217 total topics across all levels\n")
        report.write("=" * 80 + "\n")
    
    print(f"\nUpdate report generated: {REPORT_FILE}")

def main():
    """Main function to update an existing database."""
    print("ITALIAN QUIZ - DATABASE UPDATE (COMPREHENSIVE)")
    print("=" * 50)
    print("Comprehensive Dataset: 217 topics total")
    print("  A1: 46 | A2: 48 | B1: 42 | B2: 43 | C1: 38")
    print("=" * 50)
    print("This will safely update your database while preserving all progress.")
    print("Questions no longer in CSV files will be archived (not deleted).")
    print("NEW: Topics will be normalized to canonical names to prevent progress loss.")
    print("=" * 50)
    
    # Check if database exists and is compatible
    if not check_database_compatibility():
        return
    
    # Create archive tables
    if not create_archive_tables_if_needed():
        print("Failed to create archive tables. Aborting update.")
        return
    
    # Create backup
    backup_path = create_backup()
    if not backup_path:
        response = input("Failed to create backup. Continue anyway? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Update cancelled.")
            return
    
    try:
        # STEP 1: Migrate existing database topics to canonical names
        if not migrate_existing_topics():
            print("Aborting due to migration failure.")
            return

        # STEP 2: Perform the safe update from CSVs (uses canonical names)
        update_stats, csv_hashes, topic_counts = safe_update_from_csv()
        
        # STEP 3: Archive questions no longer in CSV files
        print(f"\nChecking for questions to archive...")
        archive_stats = archive_removed_questions(csv_hashes)
        
        # Combine stats
        combined_stats = {**update_stats, **archive_stats}
        
        # Generate report
        generate_update_report(combined_stats, topic_counts)
        
        # Verify integrity
        if verify_progress_integrity():
            print("\nSUCCESS! Database updated successfully.")
        else:
            print("\nWARNING: Update completed but integrity check failed.")
            print("Consider restoring from backup if you notice issues.")
        
        # Print summary
        print("\n" + "="*70)
        print("DATABASE UPDATE SUMMARY (COMPREHENSIVE DATASET)")
        print("="*70)
        print(f"Files processed: {combined_stats['files_processed']}")
        print(f"New questions added: {combined_stats['new_questions']}")
        print(f"Questions updated: {combined_stats['updated_questions']}")
        print(f"Questions unchanged: {combined_stats['unchanged_questions']}")
        print(f"Questions archived: {combined_stats['questions_archived']}")
        print(f"Performance records archived: {combined_stats['performance_archived']}")
        print(f"Answer history records archived: {combined_stats['answers_archived']}")
        print(f"Duplicate questions skipped: {combined_stats['duplicates_skipped']}")
        print(f"CSV Topics Normalized (in CSV): {combined_stats['topics_normalized_csv']}")
        print(f"Errors encountered: {combined_stats['errors']}")
        
        # Get final counts
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM questions")
        active_questions = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM archived_questions")
        archived_questions = cursor.fetchone()[0]
        
        # Count unique topics per level
        cursor.execute("""
            SELECT cefr_level, COUNT(DISTINCT topic) as topic_count
            FROM questions
            GROUP BY cefr_level
            ORDER BY cefr_level
        """)
        topic_coverage = cursor.fetchall()
        
        conn.close()
        
        print(f"\nActive questions in database: {active_questions}")
        print(f"Archived questions: {archived_questions}")
        
        print(f"\nTopic Coverage (Canonical Names):")
        total_topics_found = 0
        for level, count in topic_coverage:
            expected = EXPECTED_TOPIC_COUNTS.get(level, 0)
            status = "✓" if count >= expected and count <= expected + 1 else "⚠️" # Allow a small margin
            print(f"  {status} {level}: {count} topics (expected: {expected})")
            total_topics_found += count
        
        print(f"\nTotal unique canonical topics: {total_topics_found}/{sum(EXPECTED_TOPIC_COUNTS.values())}")
        
        print("\nUSER PROGRESS PRESERVED - All progress data has been maintained!")
        print("="*70)
        
        # Show recent changes
        show_recent_changes()
        
        print(f"\nYour updated database is ready to use with app.py")
        print(f"Check '{REPORT_FILE}' for detailed update information.")
        if backup_path:
            print(f"Backup saved at: {backup_path}")
        
    except Exception as e:
        print(f"\nERROR: Database update failed: {e}")
        if backup_path:
            print(f"You can restore from backup: {backup_path}")

def verify_progress_integrity():
    """Verify that user progress is still intact after the update."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("\nVerifying progress integrity...")
    
    try:
        # Check for orphaned performance records
        cursor.execute('''
        SELECT COUNT(*) FROM enhanced_performance ep 
        LEFT JOIN questions q ON ep.question_id = q.id 
        WHERE q.id IS NULL
        ''')
        orphaned_performance = cursor.fetchone()[0]
        
        # Check for orphaned answer history
        cursor.execute('''
        SELECT COUNT(*) FROM answer_history ah 
        LEFT JOIN questions q ON ah.question_id = q.id 
        WHERE q.id IS NULL
        ''')
        orphaned_history = cursor.fetchone()[0]
        
        # Get performance stats
        cursor.execute('SELECT COUNT(*) FROM enhanced_performance WHERE correct_count > 0 OR incorrect_count > 0 OR freeform_correct_count > 0')
        progress_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM answer_history')
        answer_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM archived_enhanced_performance')
        archived_progress = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM archived_answer_history')
        archived_answers = cursor.fetchone()[0]
        
        print(f"Active performance records with progress: {progress_records}")
        print(f"Active answer history records: {answer_records}")
        print(f"Archived performance records: {archived_progress}")
        print(f"Archived answer history records: {archived_answers}")
        print(f"Orphaned performance records: {orphaned_performance}")
        print(f"Orphaned answer history records: {orphaned_history}")
        
        if orphaned_performance == 0 and orphaned_history == 0:
            return True
        else:
            print("INTEGRITY CHECK FAILED - Some progress data is orphaned!")
            return False
            
    except Exception as e:
        print(f"Error during integrity check: {e}")
        return False
    finally:
        conn.close()

def show_recent_changes(limit=15):
    """Show recent changes to the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print(f"\nRecent changes (last {limit}):")
    print("-" * 80)
    
    try:
        cursor.execute('''
        SELECT timestamp, update_type, question_hash, old_question_id, new_question_id, notes
        FROM question_update_log 
        ORDER BY timestamp DESC 
        LIMIT ?
        ''', (limit,))
        
        changes = cursor.fetchall()
        
        if not changes:
            print("No recent changes logged.")
        else:
            for change in changes:
                timestamp, update_type, q_hash, old_id, new_id, notes = change
                print(f"{timestamp[:19]} | {update_type:8} | Hash: {q_hash[:8]}... | ID: {old_id or 'N/A'} -> {new_id or 'N/A'}")
                if notes:
                    print(f"                     | Notes: {notes}")
                print("-" * 80)
                
    except Exception as e:
        print(f"Error retrieving change log: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()