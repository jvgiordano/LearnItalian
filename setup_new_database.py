#!/usr/bin/env python3
"""
Complete setup for a new Italian Quiz database.
Creates database structure and populates it with questions from CSV files.
Updated to handle the new CSV format with hints and alternate responses.

Usage: python setup_database.py
"""

import sqlite3
import csv
import os
import hashlib
from datetime import datetime

# Configuration
DB_FILE = "italian_quiz.db"
CSV_FILES = [
    "data/italian_A1.csv",
    "data/italian_A2.csv", 
    "data/italian_B1.csv",
    "data/italian_B2.csv",
    "data/italian_C1.csv"
]
REPORT_FILE = "REPORT.txt"

def generate_question_hash(question_text, english_translation, option_a, option_b, option_c, option_d, correct_option, cefr_level, topic):
    """Generate a stable hash for a question based on its core content."""
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

def create_database_structure():
    """Create the complete database structure with new columns for hints and alternate responses."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Table 1: Questions - Enhanced with hint and alternate_correct_responses columns
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_hash TEXT UNIQUE NOT NULL,
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
        UNIQUE(question_text, cefr_level, topic)
    )
    ''')

    # Table 2: Enhanced Performance Tracking
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS enhanced_performance (
        question_id INTEGER PRIMARY KEY,
        correct_count INTEGER DEFAULT 0,
        incorrect_count INTEGER DEFAULT 0,
        last_seen TIMESTAMP,
        next_review TIMESTAMP,
        mastery_level INTEGER DEFAULT 0,
        freeform_correct_count INTEGER DEFAULT 0,
        freeform_incorrect_count INTEGER DEFAULT 0,
        partial_correct_count INTEGER DEFAULT 0,
        FOREIGN KEY (question_id) REFERENCES questions (id)
    )
    ''')

    # Table 3: Topic-specific performance tracking
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

    # Table 4: User's overall progress tracking
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_progress (
        id INTEGER PRIMARY KEY,
        estimated_level TEXT DEFAULT 'A1',
        total_questions_answered INTEGER DEFAULT 0,
        last_assessment TIMESTAMP,
        level_confidence REAL DEFAULT 0.5
    )
    ''')

    # Table 5: Mastery Statistics per Level
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS level_stats (
        level TEXT PRIMARY KEY,
        history TEXT NOT NULL
    )
    ''')

    # Table 6: Quiz History for the Graph
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS quiz_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        score INTEGER NOT NULL,
        total_questions INTEGER NOT NULL,
        session_id TEXT
    )
    ''')

    # Table 7: Detailed Answer History
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

    # Table 8: Daily stats
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        total_coverage REAL DEFAULT 0,
        total_mastery REAL DEFAULT 0,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Table 9: Question Update Log
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

    # Create indexes
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_question_hash ON questions(question_hash)')

    conn.commit()
    conn.close()
    print("Database structure created successfully.")

def populate_from_csv_files():
    """Populate the database with questions from CSV files and generate a report."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    total_inserted = 0
    total_errors = 0
    topic_counts = {}  # Dictionary to store topic counts by level
    
    for filename in CSV_FILES:
        if not os.path.exists(filename):
            print(f"Warning: '{filename}' not found. Skipping.")
            continue

        print(f"Processing '{filename}'...")
        
        try:
            with open(filename, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                # Verify expected columns are present (complete_sentence is optional/ignored)
                expected_columns = [
                    'question_text', 'english_translation', 'hint', 'alternate_correct_responses',
                    'option_a', 'option_b', 'option_c', 'option_d', 'correct_option', 
                    'cefr_level', 'topic', 'explanation', 'resource'
                ]
                
                if not all(col in csv_reader.fieldnames for col in expected_columns):
                    print(f"Error: '{filename}' missing required columns.")
                    print(f"Expected: {expected_columns}")
                    print(f"Found: {csv_reader.fieldnames}")
                    continue
                
                file_inserted = 0
                file_errors = 0
                
                for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 for header
                    try:
                        # Validate required fields (hint, alternate_correct_responses, and resource can be empty)
                        required_fields = ['question_text', 'english_translation', 'option_a', 
                                         'option_b', 'option_c', 'option_d', 'correct_option', 
                                         'cefr_level', 'topic', 'explanation']
                        
                        if not all(row.get(col, '').strip() for col in required_fields):
                            print(f"Warning: Row {row_num} in '{filename}' has empty required fields. Skipping.")
                            file_errors += 1
                            continue
                        
                        # Validate correct_option is one of A, B, C, D
                        if row['correct_option'].upper() not in ['A', 'B', 'C', 'D']:
                            print(f"Warning: Row {row_num} in '{filename}' has invalid correct_option '{row['correct_option']}'. Skipping.")
                            file_errors += 1
                            continue
                        
                        # Generate hash for this question
                        question_hash = generate_question_hash(
                            row['question_text'], row['english_translation'], 
                            row['option_a'], row['option_b'], row['option_c'], row['option_d'],
                            row['correct_option'], row['cefr_level'], row['topic']
                        )
                        
                        current_time = datetime.now().isoformat()
                        
                        # Handle optional columns
                        hint_value = row.get('hint', '').strip()
                        alternate_value = row.get('alternate_correct_responses', '').strip()
                        resource_value = row.get('resource', '').strip()
                        
                        cursor.execute('''
                        INSERT OR IGNORE INTO questions 
                        (question_hash, question_text, english_translation, hint, alternate_correct_responses,
                         option_a, option_b, option_c, option_d, correct_option, cefr_level, topic, 
                         explanation, resource, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            question_hash,
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
                            resource_value,
                            current_time,
                            current_time
                        ))
                        
                        if cursor.rowcount > 0:
                            file_inserted += 1
                            
                            # Track topic counts for report
                            level = row['cefr_level'].strip()
                            topic = row['topic'].strip()
                            
                            if level not in topic_counts:
                                topic_counts[level] = {}
                            if topic not in topic_counts[level]:
                                topic_counts[level][topic] = 0
                            topic_counts[level][topic] += 1
                            
                            # Log the creation
                            cursor.execute('''
                            INSERT INTO question_update_log 
                            (question_hash, old_question_id, new_question_id, update_type, timestamp, notes)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''', (question_hash, None, cursor.lastrowid, 'created', 
                                 current_time, f'Initial load from {filename} row {row_num}'))
                        
                    except Exception as e:
                        print(f"Error processing row {row_num} in '{filename}': {e}")
                        file_errors += 1
                        continue
                
                total_inserted += file_inserted
                total_errors += file_errors
                print(f"  Added {file_inserted} questions from '{filename}' ({file_errors} errors)")
                
        except Exception as e:
            print(f"Error reading '{filename}': {e}")
            continue

    conn.commit()
    final_count = cursor.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    conn.close()

    print(f"\nDatabase population complete!")
    print(f"Total questions added: {total_inserted}")
    print(f"Total errors encountered: {total_errors}")
    print(f"Final question count: {final_count}")
    
    return topic_counts

def generate_report(topic_counts):
    """Generate a report showing question counts by topic and level."""
    with open(REPORT_FILE, 'w', encoding='utf-8') as report:
        report.write("ITALIAN QUIZ DATABASE REPORT\n")
        report.write("=" * 80 + "\n")
        report.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report.write("=" * 80 + "\n\n")
        
        # Sort levels in order
        level_order = ['A1', 'A2', 'B1', 'B2', 'C1']
        
        for level in level_order:
            if level in topic_counts:
                report.write(f"\nLEVEL {level}\n")
                report.write("-" * 40 + "\n")
                
                # Sort topics alphabetically
                sorted_topics = sorted(topic_counts[level].items())
                total_for_level = 0
                
                for topic, count in sorted_topics:
                    report.write(f"  {topic}: {count} questions\n")
                    total_for_level += count
                
                report.write(f"  TOTAL FOR {level}: {total_for_level} questions\n")
        
        # Overall summary
        report.write("\n" + "=" * 80 + "\n")
        report.write("OVERALL SUMMARY\n")
        report.write("-" * 40 + "\n")
        
        grand_total = 0
        for level in level_order:
            if level in topic_counts:
                level_total = sum(topic_counts[level].values())
                report.write(f"  {level}: {level_total} questions\n")
                grand_total += level_total
        
        report.write(f"\n  GRAND TOTAL: {grand_total} questions\n")
        report.write("=" * 80 + "\n")
    
    print(f"\nReport generated: {REPORT_FILE}")

def verify_database():
    """Verify the database was created correctly."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get overview statistics
    cursor.execute("SELECT cefr_level, COUNT(*) as count FROM questions GROUP BY cefr_level ORDER BY cefr_level")
    level_counts = cursor.fetchall()
    
    print(f"\nDatabase verification:")
    print("Questions by CEFR Level:")
    for row in level_counts:
        print(f"  {row['cefr_level']}: {row['count']} questions")
    
    # Check for questions without hashes (should be zero)
    cursor.execute("SELECT COUNT(*) FROM questions WHERE question_hash IS NULL OR question_hash = ''")
    missing_hashes = cursor.fetchone()[0]
    
    if missing_hashes == 0:
        print("All questions have stable identifiers.")
    else:
        print(f"Warning: {missing_hashes} questions missing hashes!")
    
    # Check hint and alternate response statistics
    cursor.execute("SELECT COUNT(*) FROM questions WHERE hint IS NOT NULL AND hint != ''")
    questions_with_hints = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM questions WHERE alternate_correct_responses IS NOT NULL AND alternate_correct_responses != ''")
    questions_with_alternates = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM questions WHERE resource IS NOT NULL AND resource != ''")
    questions_with_resources = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM questions")
    total_questions = cursor.fetchone()[0]
    
    print(f"Questions with hints: {questions_with_hints}/{total_questions}")
    print(f"Questions with alternate responses: {questions_with_alternates}/{total_questions}")
    print(f"Questions with resource links: {questions_with_resources}/{total_questions}")
    
    # Show sample question
    cursor.execute("SELECT * FROM questions LIMIT 1")
    sample = cursor.fetchone()
    
    if sample:
        print(f"\nSample Question (ID: {sample['id']}):")
        print(f"  Question: {sample['question_text']}")
        print(f"  English: {sample['english_translation']}")
        print(f"  Hint: {sample['hint'] or 'None'}")
        print(f"  Alternate Answer: {sample['alternate_correct_responses'] or 'None'}")
        print(f"  Level: {sample['cefr_level']} | Topic: {sample['topic']}")
        print(f"  Resource: {sample['resource'] or 'None'}")
        print(f"  Hash: {sample['question_hash']}")
    
    conn.close()

def main():
    """Main function to set up a new database."""
    print("ITALIAN QUIZ - NEW DATABASE SETUP")
    print("=" * 50)
    print("This will create a fresh database and populate it with questions.")
    print("WARNING: This will overwrite any existing database file!")
    print("=" * 50)
    
    if os.path.exists(DB_FILE):
        response = input(f"\nDatabase '{DB_FILE}' already exists. Overwrite it? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Setup cancelled.")
            return
        
        # Create backup before overwriting
        backup_name = f"{DB_FILE}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.rename(DB_FILE, backup_name)
        print(f"Existing database backed up to: {backup_name}")
    
    try:
        print("\n1. Creating database structure...")
        create_database_structure()
        
        print("\n2. Populating with questions from CSV files...")
        topic_counts = populate_from_csv_files()
        
        print("\n3. Generating report...")
        generate_report(topic_counts)
        
        print("\n4. Verifying database...")
        verify_database()
        
        print(f"\nSUCCESS! New database '{DB_FILE}' is ready to use.")
        print(f"Check '{REPORT_FILE}' for detailed question counts by topic and level.")
        print("You can now run app.py to start the quiz application.")
        
    except Exception as e:
        print(f"\nERROR: Database setup failed: {e}")
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        print("Cleaned up incomplete database file.")

if __name__ == "__main__":
    main()