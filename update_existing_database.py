#!/usr/bin/env python3
"""
Safe update of existing Italian Quiz database - COMPREHENSIVE VERSION
Preserves all user progress while updating questions from CSV files.
Handles the comprehensive CSV format with all fields including complete_sentence.

Compatible with comprehensive dataset generator (217 total topics):
- A1: 46 topics
- A2: 48 topics  
- B1: 42 topics
- B2: 43 topics
- C1: 38 topics

Usage: python update_existing_database.py
"""

import sqlite3
import csv
import os
import hashlib
import shutil
from datetime import datetime

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
    'A1': 46,
    'A2': 48,
    'B1': 42,
    'B2': 43,
    'C1': 38
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
        topic.strip().lower()
    ]
    
    content_string = "|".join(content_parts)
    return hashlib.sha256(content_string.encode('utf-8')).hexdigest()[:16]

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
        print("Use setup_database.py to create a new database.")
        return False
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        # Check if questions table has required columns
        cursor.execute("PRAGMA table_info(questions)")
        columns = [col[1] for col in cursor.fetchall()]
        
        required_columns = ['question_hash', 'created_at', 'updated_at']
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            print(f"Database is missing required columns: {missing_columns}")
            print("The database needs to be migrated for safe updates.")
            print("Please run the migration script first.")
            return False
        
        # Check if question_update_log table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='question_update_log'")
        if not cursor.fetchone():
            print("Database is missing the question_update_log table.")
            print("The database needs to be migrated for safe updates.")
            return False
        
        # Add new columns if they don't exist
        columns_added = False
        
        if 'hint' not in columns:
            print("Adding missing 'hint' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN hint TEXT")
            columns_added = True
            print("Hint column added successfully.")
        
        if 'alternate_correct_responses' not in columns:
            print("Adding missing 'alternate_correct_responses' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN alternate_correct_responses TEXT")
            columns_added = True
            print("Alternate correct responses column added successfully.")
        
        if 'resource' not in columns:
            print("Adding missing 'resource' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN resource TEXT")
            columns_added = True
            print("Resource column added successfully.")
        
        if 'complete_sentence' not in columns:
            print("Adding missing 'complete_sentence' column to questions table...")
            cursor.execute("ALTER TABLE questions ADD COLUMN complete_sentence TEXT")
            columns_added = True
            print("Complete sentence column added successfully.")
        
        if columns_added:
            conn.commit()
        
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
            if hash_val not in csv_hashes:
                questions_to_archive.append((hash_val, question_id))
        
        if not questions_to_archive:
            print("No questions need to be removed.")
            return stats
        
        print(f"Found {len(questions_to_archive)} questions to archive/remove...")
        
        for question_hash, question_id in questions_to_archive:
            try:
                # Archive the question itself (with complete_sentence if it exists)
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
                
                # Archive enhanced performance data if it exists
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
                
                # Archive answer history if it exists
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
    """Safely update the database from CSV files while preserving user progress."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    stats = {
        'new_questions': 0,
        'updated_questions': 0,
        'unchanged_questions': 0,
        'errors': 0,
        'files_processed': 0,
        'duplicates_skipped': 0
    }
    
    topic_counts = {}  # For report generation
    
    # Get existing questions for comparison
    cursor.execute('''
        SELECT question_hash, id, complete_sentence, question_text, english_translation, 
               hint, alternate_correct_responses, option_a, option_b, option_c, option_d, 
               correct_option, cefr_level, topic, explanation, resource 
        FROM questions
    ''')
    existing_questions = {row[0]: row for row in cursor.fetchall()}  # hash -> full_record
    
    print("Starting safe database update...")
    print(f"Found {len(existing_questions)} existing questions")
    
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
                
                # Verify expected columns are present (comprehensive format)
                expected_columns = [
                    'complete_sentence', 'question_text', 'english_translation', 'hint', 
                    'alternate_correct_responses', 'option_a', 'option_b', 'option_c', 
                    'option_d', 'correct_option', 'cefr_level', 'topic', 'explanation', 'resource'
                ]
                
                missing_columns = [col for col in expected_columns if col not in csv_reader.fieldnames]
                
                if missing_columns:
                    print(f"Warning: '{filename}' missing columns: {missing_columns}")
                    print(f"   Expected: {expected_columns}")
                    print(f"   Found: {csv_reader.fieldnames}")
                    # Continue anyway - we'll handle missing columns gracefully
                
                for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 for header
                    try:
                        # Validate required fields (core fields only)
                        required_fields = ['question_text', 'english_translation', 'option_a', 
                                         'option_b', 'option_c', 'option_d', 'correct_option', 
                                         'cefr_level', 'topic', 'explanation']
                        
                        if not all(row.get(col, '').strip() for col in required_fields):
                            print(f"Warning: Row {row_num} in '{filename}' has empty required fields. Skipping.")
                            stats['errors'] += 1
                            continue
                        
                        # Validate correct_option
                        if row['correct_option'].upper() not in ['A', 'B', 'C', 'D']:
                            print(f"Warning: Row {row_num} in '{filename}' has invalid correct_option '{row['correct_option']}'. Skipping.")
                            stats['errors'] += 1
                            continue
                        
                        # Generate hash for this question
                        question_hash = generate_question_hash(
                            row['question_text'], row['english_translation'], 
                            row['option_a'], row['option_b'], row['option_c'], row['option_d'],
                            row['correct_option'], row['cefr_level'], row['topic']
                        )
                        
                        # Add to set of CSV hashes
                        csv_question_hashes.add(question_hash)
                        
                        # Track for report
                        level = row['cefr_level'].strip()
                        topic = row['topic'].strip()
                        if level not in topic_counts:
                            topic_counts[level] = {}
                        if topic not in topic_counts[level]:
                            topic_counts[level][topic] = 0
                        topic_counts[level][topic] += 1
                        
                        # Normalize values (handle optional fields gracefully)
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
                            row['cefr_level'].strip(),
                            row['topic'].strip(),
                            row['explanation'].strip(),
                            resource_value
                        )
                        
                        if question_hash in existing_questions:
                            # Question exists - check if it needs updating
                            existing_record = existing_questions[question_hash]
                            existing_values = existing_record[2:]  # Skip hash and id
                            
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
                            # New question - try to insert with new ID
                            try:
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
                                
                            except sqlite3.IntegrityError as e:
                                if "UNIQUE constraint failed" in str(e):
                                    # This is a duplicate based on (question_text, cefr_level, topic)
                                    stats['duplicates_skipped'] += 1
                                    # Silently skip - this is expected behavior
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
        report.write(f"Errors encountered: {stats['errors']}\n")
        report.write("\n")
        
        # Question counts by level and topic
        report.write("CURRENT QUESTION COUNTS BY LEVEL AND TOPIC\n")
        report.write("-" * 40 + "\n")
        
        level_order = ['A1', 'A2', 'B1', 'B2', 'C1']
        
        for level in level_order:
            if level in topic_counts:
                report.write(f"\nLEVEL {level} (Expected: {EXPECTED_TOPIC_COUNTS.get(level, '?')} topics)\n")
                report.write("-" * 20 + "\n")
                
                sorted_topics = sorted(topic_counts[level].items())
                total_for_level = 0
                unique_topics = len(sorted_topics)
                
                for topic, count in sorted_topics:
                    report.write(f"  {topic}: {count} questions\n")
                    total_for_level += count
                
                report.write(f"  TOTAL FOR {level}: {total_for_level} questions across {unique_topics} topics\n")
                
                # Show topic coverage vs expected
                expected = EXPECTED_TOPIC_COUNTS.get(level, 0)
                if unique_topics < expected:
                    report.write(f"  ⚠️  WARNING: Only {unique_topics} topics found, expected {expected}\n")
                elif unique_topics > expected:
                    report.write(f"  ℹ️  INFO: {unique_topics} topics found, expected {expected} (may include new topics)\n")
                else:
                    report.write(f"  ✓ Complete topic coverage: {unique_topics}/{expected}\n")
        
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
                report.write(f"  {level}: {level_total} questions across {level_topics} topics\n")
                grand_total += level_total
                total_topics += level_topics
        
        report.write(f"\n  GRAND TOTAL: {grand_total} active questions across {total_topics} topics\n")
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
        # Perform the safe update and get CSV hashes
        update_stats, csv_hashes, topic_counts = safe_update_from_csv()
        
        # Archive questions no longer in CSV files
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
        
        print(f"\nTopic Coverage:")
        total_topics_found = 0
        for level, count in topic_coverage:
            expected = EXPECTED_TOPIC_COUNTS.get(level, 0)
            status = "✓" if count >= expected else "⚠️"
            print(f"  {status} {level}: {count} topics (expected: {expected})")
            total_topics_found += count
        
        print(f"\nTotal unique topics: {total_topics_found}/217")
        
        print("\nUSER PROGRESS PRESERVED - All progress data has been maintained!")
        print("Removed questions are archived and can be recovered if needed.")
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
            print("INTEGRITY CHECK PASSED - No broken progress links!")
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