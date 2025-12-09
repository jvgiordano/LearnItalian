#!/usr/bin/env python3
"""
Update Italian Quiz Database from CSV Files
Matches setup_new_database.py hash function exactly.

Purpose:
- Update existing questions (preserves progress on unchanged questions)
- Add new questions from CSV
- Remove questions no longer in CSV (archives with progress data)
- Handle topic name changes gracefully

Usage: python update_database.py
"""

import sqlite3
import csv
import os
import hashlib
import shutil
from datetime import datetime
from pathlib import Path

# Configuration
DB_FILE = "italian_quiz.db"
BACKUP_DIR = "backups"
DATA_DIR = "data"
LEVELS = ['A1', 'A2', 'B1', 'B2', 'C1']
REPORT_FILE = "UPDATE_REPORT.txt"

def generate_hash(level: str, question_text: str) -> str:
    """
    Generate hash exactly like setup_new_database.py does.
    Hash = MD5 of "level:question_text" (first 16 chars)
    """
    return hashlib.md5(f"{level}:{question_text}".encode()).hexdigest()[:16]

def create_backup() -> str:
    """Create timestamped backup of database."""
    if not os.path.exists(DB_FILE):
        print(f"⚠️  No database file found: {DB_FILE}")
        return None
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f"{BACKUP_DIR}/italian_quiz_backup_{timestamp}.db"
    
    shutil.copy2(DB_FILE, backup_file)
    print(f"✅ Backup created: {backup_file}")
    return backup_file

def find_csv_files() -> dict:
    """Find CSV files for each level."""
    files = {}
    
    for level in LEVELS:
        # Try both Italian_A1.csv and italian_A1.csv
        for filename in [f"Italian_{level}.csv", f"italian_{level}.csv"]:
            filepath = Path(DATA_DIR) / filename
            if filepath.exists():
                files[level] = filepath
                break
    
    return files

def load_csv_questions(csv_files: dict) -> dict:
    """
    Load all questions from CSV files with validation.
    Returns: {hash: {question_data}}
    Skips invalid questions like setup script does.
    """
    csv_questions = {}
    stats = {
        'total_rows': 0,
        'valid_questions': 0,
        'skipped_missing_fields': 0,
        'skipped_invalid_option': 0,
        'skipped_duplicates': 0
    }
    
    for level, filepath in csv_files.items():
        print(f"📂 Reading {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                stats['total_rows'] += 1
                
                # 1. Basic Validation - same as setup script
                required = ['question_text', 'correct_option', 'option_a', 'topic']
                if any(not row.get(field) for field in required):
                    stats['skipped_missing_fields'] += 1
                    continue
                
                # 2. Validate Correct Option - same as setup script
                correct = row['correct_option'].strip().upper()
                if correct not in ['A', 'B', 'C', 'D']:
                    stats['skipped_invalid_option'] += 1
                    continue
                
                # 3. Generate hash using setup script's method
                q_text = row['question_text'].strip()
                question_hash = generate_hash(level, q_text)
                
                # 4. Skip duplicates within CSV
                if question_hash in csv_questions:
                    stats['skipped_duplicates'] += 1
                    continue
                
                # Store all question data
                csv_questions[question_hash] = {
                    'complete_sentence': row.get('complete_sentence', '').strip(),
                    'question_text': q_text,
                    'english_translation': row.get('english_translation', '').strip(),
                    'hint': row.get('hint', '').strip(),
                    'alternate_correct_responses': row.get('alternate_correct_responses', '').strip(),
                    'option_a': row.get('option_a', '').strip(),
                    'option_b': row.get('option_b', '').strip(),
                    'option_c': row.get('option_c', '').strip(),
                    'option_d': row.get('option_d', '').strip(),
                    'correct_option': correct,
                    'cefr_level': level,
                    'topic': row.get('topic', '').strip(),
                    'explanation': row.get('explanation', '').strip(),
                    'resource': row.get('resource', '').strip(),
                }
                
                stats['valid_questions'] += 1
    
    # Print validation summary
    print(f"\n📊 CSV Validation Summary:")
    print(f"   Total rows processed: {stats['total_rows']}")
    print(f"   Valid questions: {stats['valid_questions']}")
    if stats['skipped_missing_fields'] > 0:
        print(f"   ⚠️  Skipped (missing fields): {stats['skipped_missing_fields']}")
    if stats['skipped_invalid_option'] > 0:
        print(f"   ⚠️  Skipped (invalid correct_option): {stats['skipped_invalid_option']}")
    if stats['skipped_duplicates'] > 0:
        print(f"   ⚠️  Skipped (duplicates): {stats['skipped_duplicates']}")
    
    return csv_questions

def load_db_questions() -> dict:
    """
    Load all questions from database.
    Returns: {hash: {id, question_data}}
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, hash_id, complete_sentence, question_text, english_translation,
               hint, alternate_correct_responses, option_a, option_b, option_c, option_d,
               correct_option, cefr_level, topic, explanation, resource, created_at
        FROM questions
    """)
    
    db_questions = {}
    for row in cursor.fetchall():
        db_questions[row['hash_id']] = dict(row)
    
    conn.close()
    return db_questions

def compare_questions(csv_data: dict, db_data: dict) -> bool:
    """Check if question data has changed (excluding id and hash)."""
    # Compare all fields except id and hash_id
    fields = ['complete_sentence', 'question_text', 'english_translation', 'hint',
              'alternate_correct_responses', 'option_a', 'option_b', 'option_c', 'option_d',
              'correct_option', 'cefr_level', 'topic', 'explanation', 'resource']
    
    for field in fields:
        if csv_data.get(field, '') != db_data.get(field, ''):
            return False
    
    return True

def check_and_add_columns():
    """Ensure archive tables exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Ensure archive tables exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archived_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_question_id INTEGER NOT NULL,
            hash_id TEXT NOT NULL,
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
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            removal_reason TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archived_enhanced_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_question_id INTEGER NOT NULL,
            hash_id TEXT NOT NULL,
            correct_count INTEGER DEFAULT 0,
            incorrect_count INTEGER DEFAULT 0,
            partial_correct_count INTEGER DEFAULT 0,
            freeform_correct_count INTEGER DEFAULT 0,
            last_seen TIMESTAMP,
            last_answered_at TIMESTAMP,
            next_review_at TIMESTAMP,
            mastery_level REAL DEFAULT 0.0,
            history_string TEXT DEFAULT '',
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archived_answer_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_answer_id INTEGER NOT NULL,
            original_question_id INTEGER NOT NULL,
            hash_id TEXT NOT NULL,
            user_answer TEXT,
            correct_answer TEXT,
            is_correct BOOLEAN,
            timestamp DATETIME,
            quiz_session_id TEXT,
            cefr_level TEXT NOT NULL,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

def update_database(csv_questions: dict, db_questions: dict) -> dict:
    """
    Update database with CSV data.
    Returns statistics about changes.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    stats = {
        'unchanged': 0,
        'updated': 0,
        'added': 0,
        'removed': 0,
        'errors': 0
    }
    
    current_time = datetime.now().isoformat()
    
    # Process CSV questions
    for question_hash, csv_data in csv_questions.items():
        try:
            if question_hash in db_questions:
                # Question exists - check if it needs updating
                db_data = db_questions[question_hash]
                
                if compare_questions(csv_data, db_data):
                    stats['unchanged'] += 1
                else:
                    # Update existing question
                    cursor.execute("""
                        UPDATE questions
                        SET complete_sentence = ?, question_text = ?, english_translation = ?,
                            hint = ?, alternate_correct_responses = ?,
                            option_a = ?, option_b = ?, option_c = ?, option_d = ?,
                            correct_option = ?, cefr_level = ?, topic = ?,
                            explanation = ?, resource = ?
                        WHERE hash_id = ?
                    """, (
                        csv_data['complete_sentence'],
                        csv_data['question_text'],
                        csv_data['english_translation'],
                        csv_data['hint'],
                        csv_data['alternate_correct_responses'],
                        csv_data['option_a'],
                        csv_data['option_b'],
                        csv_data['option_c'],
                        csv_data['option_d'],
                        csv_data['correct_option'],
                        csv_data['cefr_level'],
                        csv_data['topic'],
                        csv_data['explanation'],
                        csv_data['resource'],
                        question_hash
                    ))
                    
                    stats['updated'] += 1
                    print(f"  ✏️  Updated: {csv_data['question_text'][:50]}...")
            else:
                # New question - add it
                cursor.execute("""
                    INSERT INTO questions (
                        hash_id, complete_sentence, question_text, english_translation,
                        hint, alternate_correct_responses,
                        option_a, option_b, option_c, option_d,
                        correct_option, cefr_level, topic,
                        explanation, resource, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    question_hash,
                    csv_data['complete_sentence'],
                    csv_data['question_text'],
                    csv_data['english_translation'],
                    csv_data['hint'],
                    csv_data['alternate_correct_responses'],
                    csv_data['option_a'],
                    csv_data['option_b'],
                    csv_data['option_c'],
                    csv_data['option_d'],
                    csv_data['correct_option'],
                    csv_data['cefr_level'],
                    csv_data['topic'],
                    csv_data['explanation'],
                    csv_data['resource'],
                    current_time
                ))
                
                question_id = cursor.lastrowid
                
                # Initialize performance tracking
                cursor.execute("""
                    INSERT INTO enhanced_performance (question_id) VALUES (?)
                """, (question_id,))
                
                stats['added'] += 1
                print(f"  ➕ Added: {csv_data['question_text'][:50]}...")
        
        except Exception as e:
            print(f"❌ Error processing {csv_data.get('question_text', 'unknown')}: {e}")
            stats['errors'] += 1
    
    # Find removed questions (in DB but not in CSV)
    removed_hashes = set(db_questions.keys()) - set(csv_questions.keys())
    
    if removed_hashes:
        print(f"\n⚠️  Found {len(removed_hashes)} questions to remove")
        
        # Safety check - don't remove more than 50% of questions
        if len(removed_hashes) > len(db_questions) * 0.5:
            print(f"❌ SAFETY: Refusing to remove {len(removed_hashes)}/{len(db_questions)} questions (>50%)")
            print(f"   This seems like an error. No questions will be removed.")
        else:
            for removed_hash in removed_hashes:
                db_data = db_questions[removed_hash]
                
                # Archive the question and its progress
                try:
                    # Archive question
                    cursor.execute("""
                        INSERT INTO archived_questions (
                            original_question_id, hash_id, complete_sentence, question_text,
                            english_translation, hint, alternate_correct_responses,
                            option_a, option_b, option_c, option_d, correct_option,
                            cefr_level, topic, explanation, resource,
                            created_at, archived_at, removal_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        db_data['id'], removed_hash,
                        db_data['complete_sentence'], db_data['question_text'],
                        db_data['english_translation'], db_data['hint'],
                        db_data['alternate_correct_responses'],
                        db_data['option_a'], db_data['option_b'],
                        db_data['option_c'], db_data['option_d'],
                        db_data['correct_option'], db_data['cefr_level'],
                        db_data['topic'], db_data['explanation'],
                        db_data['resource'], db_data.get('created_at'),
                        current_time, 'No longer in CSV files'
                    ))
                    
                    # Archive performance data
                    cursor.execute("""
                        INSERT INTO archived_enhanced_performance (
                            original_question_id, hash_id, correct_count, incorrect_count,
                            partial_correct_count, freeform_correct_count,
                            last_seen, last_answered_at, next_review_at,
                            mastery_level, history_string, archived_at
                        )
                        SELECT question_id, ?, correct_count, incorrect_count,
                               partial_correct_count, freeform_correct_count,
                               last_seen, last_answered_at, next_review_at,
                               mastery_level, history_string, ?
                        FROM enhanced_performance
                        WHERE question_id = ?
                    """, (removed_hash, current_time, db_data['id']))
                    
                    # Archive answer history
                    cursor.execute("""
                        INSERT INTO archived_answer_history (
                            original_answer_id, original_question_id, hash_id,
                            user_answer, correct_answer, is_correct,
                            timestamp, quiz_session_id, cefr_level, archived_at
                        )
                        SELECT id, question_id, ?, user_answer, correct_answer,
                               is_correct, timestamp, quiz_session_id, cefr_level, ?
                        FROM answer_history
                        WHERE question_id = ?
                    """, (removed_hash, current_time, db_data['id']))
                    
                    # Delete from active tables
                    cursor.execute("DELETE FROM answer_history WHERE question_id = ?", (db_data['id'],))
                    cursor.execute("DELETE FROM enhanced_performance WHERE question_id = ?", (db_data['id'],))
                    cursor.execute("DELETE FROM questions WHERE id = ?", (db_data['id'],))
                    
                    stats['removed'] += 1
                    print(f"  🗑️  Removed: {db_data['question_text'][:50]}...")
                
                except Exception as e:
                    print(f"❌ Error removing question {db_data['id']}: {e}")
                    stats['errors'] += 1
    
    conn.commit()
    conn.close()
    
    return stats

def verify_integrity() -> bool:
    """Verify database integrity after update."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Check for orphaned performance records
    cursor.execute("""
        SELECT COUNT(*) FROM enhanced_performance ep
        LEFT JOIN questions q ON ep.question_id = q.id
        WHERE q.id IS NULL
    """)
    orphaned = cursor.fetchone()[0]
    
    conn.close()
    
    if orphaned > 0:
        print(f"⚠️  Warning: {orphaned} orphaned performance records found")
        return False
    
    return True

def generate_report(stats: dict, csv_files: dict):
    """Generate update report."""
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("ITALIAN QUIZ DATABASE UPDATE REPORT\n")
        f.write("=" * 70 + "\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("FILES PROCESSED:\n")
        for level, filepath in csv_files.items():
            f.write(f"  {level}: {filepath}\n")
        
        f.write(f"\nSTATISTICS:\n")
        f.write(f"  Unchanged questions: {stats['unchanged']}\n")
        f.write(f"  Updated questions: {stats['updated']}\n")
        f.write(f"  Added questions: {stats['added']}\n")
        f.write(f"  Removed questions: {stats['removed']}\n")
        f.write(f"  Errors: {stats['errors']}\n")
        
        # Get final counts
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM questions")
        total_questions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM archived_questions")
        total_archived = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM enhanced_performance
            WHERE correct_count > 0 OR incorrect_count > 0
        """)
        questions_with_progress = cursor.fetchone()[0]
        
        conn.close()
        
        f.write(f"\nCURRENT DATABASE:\n")
        f.write(f"  Active questions: {total_questions}\n")
        f.write(f"  Archived questions: {total_archived}\n")
        f.write(f"  Questions with progress: {questions_with_progress}\n")
        
        f.write("=" * 70 + "\n")
    
    print(f"✅ Report saved to: {REPORT_FILE}")

def main():
    print("=" * 70)
    print("ITALIAN QUIZ DATABASE UPDATE")
    print("=" * 70)
    print()
    
    # Find CSV files
    csv_files = find_csv_files()
    
    if not csv_files:
        print(f"❌ No CSV files found in {DATA_DIR}/")
        print(f"   Looking for: Italian_A1.csv, Italian_A2.csv, etc.")
        return
    
    print(f"Found CSV files for levels: {', '.join(csv_files.keys())}\n")
    
    # Ensure archive tables exist
    check_and_add_columns()
    
    # Create backup
    backup_file = create_backup()
    if not backup_file:
        response = input("No backup created. Continue anyway? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Update cancelled.")
            return
    
    print()
    
    # Load data
    print("📖 Loading CSV questions...")
    csv_questions = load_csv_questions(csv_files)
    print(f"   Found {len(csv_questions)} questions in CSV files\n")
    
    print("📖 Loading database questions...")
    db_questions = load_db_questions()
    print(f"   Found {len(db_questions)} questions in database\n")
    
    # Show preview
    print("PREVIEW:")
    matching = len(set(csv_questions.keys()) & set(db_questions.keys()))
    new = len(set(csv_questions.keys()) - set(db_questions.keys()))
    removed = len(set(db_questions.keys()) - set(csv_questions.keys()))
    
    print(f"  Matching questions: {matching}")
    print(f"  New questions: {new}")
    print(f"  Removed questions: {removed}")
    print()
    
    # Confirm
    if removed > 0:
        print(f"⚠️  Warning: {removed} questions will be archived (not deleted)")
        print("   Their progress will be preserved in archived tables")
        print()
    
    response = input("Proceed with update? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Update cancelled.")
        return
    
    print("\n" + "=" * 70)
    print("UPDATING DATABASE...")
    print("=" * 70 + "\n")
    
    # Perform update
    stats = update_database(csv_questions, db_questions)
    
    print("\n" + "=" * 70)
    print("UPDATE COMPLETE")
    print("=" * 70)
    print(f"  Unchanged: {stats['unchanged']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Added: {stats['added']}")
    print(f"  Removed: {stats['removed']}")
    print(f"  Errors: {stats['errors']}")
    print()
    
    # Verify integrity
    if verify_integrity():
        print("✅ Database integrity verified")
    else:
        print("⚠️  Database integrity issues detected")
        if backup_file:
            print(f"   You can restore from: {backup_file}")
    
    # Generate report
    generate_report(stats, csv_files)
    
    print("\n✅ Update complete!")
    if backup_file:
        print(f"💾 Backup: {backup_file}")

if __name__ == "__main__":
    main()
