#!/usr/bin/env python3
"""
Italian Question Duplicate Checker - UPDATED VERSION
Now aligned with setup_new_database.py and update_existing_database.py

Key Changes:
- Uses same MD5 hash function as database scripts
- Checks duplicates within CEFR level context
- Validates required CSV fields
- Option to validate topics against official list
- Better schema compliance checking

Usage:
    python duplicate_check.py check input.csv
    python duplicate_check.py dedupe input.csv output.csv --level A1
    python duplicate_check.py merge file1.csv file2.csv output.csv --level A1
    python duplicate_check.py validate input.csv --check-topics
"""

import csv
import argparse
import sys
import os
import hashlib
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from collections import defaultdict

# For pandas - will give helpful error if not installed
try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas")
    sys.exit(1)

# Required CSV fields (from setup_new_database.py)
REQUIRED_FIELDS = ['question_text', 'correct_option', 'option_a', 'topic']
ALL_EXPECTED_FIELDS = [
    'complete_sentence', 'question_text', 'english_translation', 'hint',
    'alternate_correct_responses', 'option_a', 'option_b', 'option_c', 'option_d',
    'correct_option', 'topic', 'explanation', 'resource'
]

VALID_CORRECT_OPTIONS = ['A', 'B', 'C', 'D']
CEFR_LEVELS = ['A1', 'A2', 'B1', 'B2', 'C1']


class ItalianDuplicateChecker:
    """
    Checks for and removes duplicate Italian questions from CSVs.
    Updated to match database script logic exactly.
    """
    
    def __init__(self, verbose=False, level=None, check_topics=False):
        self.verbose = verbose
        self.level = level  # Optional: restrict to specific CEFR level
        self.check_topics = check_topics
        self.duplicates = {'hash_duplicates': [], 'fuzzy_duplicates': []}  # Initialize as dict
        self.validation_errors = []
        self.stats = {
            'total_processed': 0,
            'unique_questions': 0,
            'duplicates_found': 0,
            'duplicates_removed': 0,
            'validation_errors': 0,
            'hash_duplicates': 0,  # Exact hash matches
            'fuzzy_duplicates': 0   # Similarity matches
        }
        
        # Load official topics if validation requested
        self.official_topics = {}
        if check_topics:
            self.load_official_topics()
    
    def load_official_topics(self):
        """Try to load official topics from make_italian_datasets.py"""
        try:
            from make_italian_datasets import TOPICS_BY_LEVEL
            self.official_topics = TOPICS_BY_LEVEL
            print(f"✅ Loaded official topics for validation")
        except ImportError:
            print("⚠️  Warning: Could not import make_italian_datasets.py")
            print("   Topic validation will be skipped")
            self.check_topics = False
    
    def generate_hash(self, level: str, question_text: str) -> str:
        """
        Generate hash EXACTLY like setup_new_database.py and update_existing_database.py
        Hash = MD5 of "level:question_text" (first 16 chars)
        """
        return hashlib.md5(f"{level}:{question_text}".encode()).hexdigest()[:16]
    
    def validate_row(self, row: pd.Series, row_num: int) -> Tuple[bool, Optional[str]]:
        """
        Validate row using same logic as setup_new_database.py
        Returns (is_valid, error_message)
        """
        # Check for header row accidentally included in data
        if str(row.get('question_text', '')).strip().lower() == 'question_text':
            return False, "Duplicate header row detected"
        
        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in row.index or pd.isna(row[field]) or not str(row[field]).strip():
                return False, f"Missing required field: {field}"
        
        # Validate correct_option
        correct = str(row['correct_option']).strip().upper()
        if correct not in VALID_CORRECT_OPTIONS:
            return False, f"Invalid correct_option: '{correct}' (must be A, B, C, or D)"
        
        # Check CEFR level if provided
        if self.level:
            row_level = str(row.get('cefr_level', '')).strip()
            if row_level and row_level != self.level:
                return False, f"Level mismatch: expected {self.level}, got {row_level}"
        
        # Validate topic against official list
        if self.check_topics and self.official_topics:
            topic = str(row['topic']).strip()
            level = str(row.get('cefr_level', self.level or 'A1')).strip()
            
            if level in self.official_topics:
                official_list = self.official_topics[level]
                if topic not in official_list:
                    # Try fuzzy match like setup script does
                    from difflib import get_close_matches
                    matches = get_close_matches(topic, official_list, n=1, cutoff=0.4)
                    if not matches:
                        return False, f"Invalid topic '{topic}' for level {level}"
        
        return True, None
    
    def check_duplicates(self, input_file: str, threshold: float = 0.85) -> Dict:
        """Check CSV for duplicates and generate report."""
        
        # Read input CSV
        df = self.read_csv_robust(input_file)
        
        if df is None:
            print(f"❌ Failed to read {input_file}")
            return self.generate_report()
        
        # Infer level from filename if not specified
        if not self.level:
            for lvl in CEFR_LEVELS:
                if lvl in input_file:
                    self.level = lvl
                    print(f"📋 Auto-detected CEFR level: {self.level}")
                    break
        
        print(f"Checking {len(df)} questions for duplicates...")
        print(f"  Hash-based (exact): {self.level or 'all levels'}")
        print(f"  Fuzzy matching threshold: {threshold:.0%}")
        
        # Track by hash (like database does)
        seen_hashes = {}
        hash_duplicates = []
        
        # Also track by fuzzy matching
        unique_questions = []
        fuzzy_duplicates = []
        
        for idx, row in df.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df)} questions...")
            
            # 1. Validate row
            is_valid, error_msg = self.validate_row(row, idx)
            if not is_valid:
                self.stats['validation_errors'] += 1
                self.validation_errors.append({
                    'row': idx,
                    'error': error_msg,
                    'question': str(row.get('question_text', 'N/A'))[:60]
                })
                continue
            
            # 2. Extract data
            level = str(row.get('cefr_level', self.level or 'A1')).strip()
            q_text = str(row['question_text']).strip()
            
            # 3. Generate hash (like database does)
            question_hash = self.generate_hash(level, q_text)
            
            # 4. Check for exact hash duplicate
            if question_hash in seen_hashes:
                self.stats['hash_duplicates'] += 1
                hash_duplicates.append({
                    'row': idx,
                    'question': q_text[:60],
                    'hash': question_hash,
                    'duplicate_of_row': seen_hashes[question_hash]['row'],
                    'level': level
                })
                continue
            
            seen_hashes[question_hash] = {
                'row': idx,
                'question': q_text,
                'level': level
            }
            
            # 5. Check for fuzzy duplicates (optional additional check)
            question_data = self.extract_question_data(row, idx)
            
            is_fuzzy_duplicate = False
            for unique_q in unique_questions:
                # Only check within same level
                if unique_q['cefr_level'] == level:
                    if self.is_fuzzy_duplicate(question_data, unique_q, threshold):
                        is_fuzzy_duplicate = True
                        self.stats['fuzzy_duplicates'] += 1
                        fuzzy_duplicates.append({
                            'row': idx,
                            'question': q_text[:60],
                            'similar_to_row': unique_q['index'],
                            'similar_to': unique_q['question_text'][:60],
                            'level': level
                        })
                        break
            
            if not is_fuzzy_duplicate:
                unique_questions.append(question_data)
                self.stats['unique_questions'] += 1
        
        # Combine all duplicates
        self.stats['duplicates_found'] = self.stats['hash_duplicates'] + self.stats['fuzzy_duplicates']
        self.duplicates = {
            'hash_duplicates': hash_duplicates,
            'fuzzy_duplicates': fuzzy_duplicates
        }
        
        return self.generate_report()
    
    def deduplicate(self, input_file: str, output_file: str, threshold: float = 0.85) -> Dict:
        """Remove duplicates from CSV using database script logic."""
        
        # Read input CSV
        df = self.read_csv_robust(input_file)
        
        if df is None:
            print(f"❌ Failed to read {input_file}")
            return None
        
        # Infer level from filename if not specified
        if not self.level:
            for lvl in CEFR_LEVELS:
                if lvl in input_file:
                    self.level = lvl
                    print(f"📋 Auto-detected CEFR level: {self.level}")
                    break
        
        print(f"Deduplicating {len(df)} questions...")
        
        seen_hashes = set()
        unique_questions = []
        unique_indices = []
        hash_duplicates = []
        fuzzy_duplicates = []
        
        for idx, row in df.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df)} questions...")
            
            # 1. Validate row
            is_valid, error_msg = self.validate_row(row, idx)
            if not is_valid:
                self.stats['validation_errors'] += 1
                self.validation_errors.append({
                    'row': idx,
                    'error': error_msg,
                    'question': str(row.get('question_text', 'N/A'))[:60]
                })
                continue
            
            # 2. Extract data
            level = str(row.get('cefr_level', self.level or 'A1')).strip()
            q_text = str(row['question_text']).strip()
            
            # 3. Generate hash
            question_hash = self.generate_hash(level, q_text)
            
            # 4. Check if hash duplicate (like database does)
            if question_hash in seen_hashes:
                self.stats['hash_duplicates'] += 1
                self.stats['duplicates_found'] += 1
                hash_duplicates.append({
                    'row': idx,
                    'question': q_text[:60],
                    'hash': question_hash,
                    'level': level
                })
                continue
            
            seen_hashes.add(question_hash)
            
            # 5. Optional: Check fuzzy duplicates
            question_data = self.extract_question_data(row, idx)
            is_fuzzy_duplicate = False
            
            for unique_q in unique_questions:
                if unique_q['cefr_level'] == level:
                    if self.is_fuzzy_duplicate(question_data, unique_q, threshold):
                        is_fuzzy_duplicate = True
                        self.stats['fuzzy_duplicates'] += 1
                        self.stats['duplicates_found'] += 1
                        fuzzy_duplicates.append({
                            'row': idx,
                            'question': q_text[:60],
                            'similar_to_row': unique_q['index'],
                            'similar_to': unique_q['question_text'][:60],
                            'level': level
                        })
                        break
            
            if not is_fuzzy_duplicate:
                unique_questions.append(question_data)
                unique_indices.append(idx)
                self.stats['unique_questions'] += 1
        
        # Store duplicates in proper format
        self.duplicates = {
            'hash_duplicates': hash_duplicates,
            'fuzzy_duplicates': fuzzy_duplicates
        }
        
        # Save deduplicated data
        df_clean = df.loc[unique_indices]
        self.stats['duplicates_removed'] = self.stats['duplicates_found']
        
        # Write to output
        try:
            df_clean.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✅ Saved {len(df_clean)} unique questions to {output_file}")
            print(f"🗑️  Removed {self.stats['duplicates_removed']} duplicates")
            print(f"   - Hash duplicates: {self.stats['hash_duplicates']}")
            print(f"   - Fuzzy duplicates: {self.stats['fuzzy_duplicates']}")
        except Exception as e:
            print(f"❌ Error saving file: {e}")
        
        return self.generate_report()
    
    def merge_and_deduplicate(self, file1: str, file2: str, output_file: str, threshold: float = 0.85) -> Dict:
        """Merge two CSV files and remove duplicates."""
        
        # Read both files
        df1 = self.read_csv_robust(file1)
        df2 = self.read_csv_robust(file2)
        
        if df1 is None or df2 is None:
            print(f"❌ Failed to read input files")
            return None
        
        print(f"Merging {len(df1)} questions from {file1}")
        print(f"     and {len(df2)} questions from {file2}")
        
        # Combine dataframes
        df_combined = pd.concat([df1, df2], ignore_index=True)
        print(f"Combined total: {len(df_combined)} questions")
        
        # Now deduplicate using database logic
        seen_hashes = set()
        unique_questions = []
        unique_indices = []
        hash_duplicates = []
        fuzzy_duplicates = []
        
        for idx, row in df_combined.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df_combined)} questions...")
            
            # Validate
            is_valid, error_msg = self.validate_row(row, idx)
            if not is_valid:
                self.stats['validation_errors'] += 1
                self.validation_errors.append({
                    'row': idx,
                    'error': error_msg,
                    'question': str(row.get('question_text', 'N/A'))[:60]
                })
                continue
            
            # Generate hash
            level = str(row.get('cefr_level', self.level or 'A1')).strip()
            q_text = str(row['question_text']).strip()
            question_hash = self.generate_hash(level, q_text)
            
            # Check hash duplicate
            if question_hash in seen_hashes:
                self.stats['hash_duplicates'] += 1
                self.stats['duplicates_found'] += 1
                hash_duplicates.append({
                    'row': idx,
                    'question': q_text[:60],
                    'hash': question_hash,
                    'level': level
                })
                continue
            
            seen_hashes.add(question_hash)
            
            # Check fuzzy duplicate
            question_data = self.extract_question_data(row, idx)
            is_fuzzy_duplicate = False
            
            for unique_q in unique_questions:
                if unique_q['cefr_level'] == level:
                    if self.is_fuzzy_duplicate(question_data, unique_q, threshold):
                        is_fuzzy_duplicate = True
                        self.stats['fuzzy_duplicates'] += 1
                        self.stats['duplicates_found'] += 1
                        fuzzy_duplicates.append({
                            'row': idx,
                            'question': q_text[:60],
                            'similar_to_row': unique_q['index'],
                            'similar_to': unique_q['question_text'][:60],
                            'level': level
                        })
                        break
            
            if not is_fuzzy_duplicate:
                unique_questions.append(question_data)
                unique_indices.append(idx)
                self.stats['unique_questions'] += 1
        
        # Store duplicates in proper format
        self.duplicates = {
            'hash_duplicates': hash_duplicates,
            'fuzzy_duplicates': fuzzy_duplicates
        }
        
        # Save merged and deduplicated data
        df_clean = df_combined.loc[unique_indices]
        self.stats['duplicates_removed'] = self.stats['duplicates_found']
        
        try:
            df_clean.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✅ Saved {len(df_clean)} unique questions to {output_file}")
            print(f"🗑️  Removed {self.stats['duplicates_removed']} duplicates during merge")
        except Exception as e:
            print(f"❌ Error saving file: {e}")
        
        return self.generate_report()
    
    def validate_csv_schema(self, input_file: str) -> Dict:
        """Validate CSV schema and content without deduplication."""
        df = self.read_csv_robust(input_file)
        
        if df is None:
            return None
        
        print(f"Validating {len(df)} questions...")
        
        # Check for required columns
        missing_required = [f for f in REQUIRED_FIELDS if f not in df.columns]
        if missing_required:
            print(f"❌ Missing required columns: {missing_required}")
        
        # Check for recommended columns
        missing_recommended = [f for f in ALL_EXPECTED_FIELDS if f not in df.columns]
        if missing_recommended:
            print(f"⚠️  Missing recommended columns: {missing_recommended}")
        
        # Validate each row
        for idx, row in df.iterrows():
            is_valid, error_msg = self.validate_row(row, idx)
            if not is_valid:
                self.stats['validation_errors'] += 1
                self.validation_errors.append({
                    'row': idx,
                    'error': error_msg,
                    'question': str(row.get('question_text', 'N/A'))[:60]
                })
            else:
                self.stats['unique_questions'] += 1
            
            self.stats['total_processed'] += 1
        
        return self.generate_report()
    
    def read_csv_robust(self, input_file: str) -> Optional[pd.DataFrame]:
        """Read CSV with multiple encoding attempts."""
        strategies = [
            {'encoding': 'utf-8'},
            {'encoding': 'utf-8', 'on_bad_lines': 'skip'},
            {'encoding': 'latin-1'},
            {'encoding': 'latin-1', 'on_bad_lines': 'skip'},
        ]
        
        for strategy in strategies:
            try:
                df = pd.read_csv(input_file, **strategy)
                return df
            except Exception:
                continue
        
        return None
    
    def extract_question_data(self, row: pd.Series, idx: int) -> Dict:
        """Extract question data."""
        level = str(row.get('cefr_level', self.level or 'A1')).strip()
        q_text = str(row.get('question_text', '')).strip()
        
        return {
            'index': idx,
            'cefr_level': level,
            'question_text': q_text,
            'complete_sentence': str(row.get('complete_sentence', '')).strip(),
            'english_translation': str(row.get('english_translation', '')).strip(),
            'topic': str(row.get('topic', '')).strip(),
            'hash': self.generate_hash(level, q_text)
        }
    
    def is_fuzzy_duplicate(self, q1: Dict, q2: Dict, threshold: float) -> bool:
        """Check if two questions are fuzzy duplicates."""
        # Must be same level
        if q1['cefr_level'] != q2['cefr_level']:
            return False
        
        # Check question text similarity
        if q1['question_text'] and q2['question_text']:
            text_sim = self.similarity_ratio(q1['question_text'], q2['question_text'])
            if text_sim > threshold:
                return True
        
        # Check complete sentence similarity
        if q1['complete_sentence'] and q2['complete_sentence']:
            complete_sim = self.similarity_ratio(q1['complete_sentence'], q2['complete_sentence'])
            if complete_sim > threshold:
                return True
        
        return False
    
    def similarity_ratio(self, str1: str, str2: str) -> float:
        """Calculate string similarity ratio."""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def generate_report(self) -> Dict:
        """Generate analysis report."""
        return {
            'statistics': self.stats,
            'duplicates': self.duplicates,
            'validation_errors': self.validation_errors[:20],
            'summary': {
                'total_questions': self.stats['total_processed'],
                'unique_questions': self.stats['unique_questions'],
                'duplicates_found': self.stats['duplicates_found'],
                'hash_duplicates': self.stats['hash_duplicates'],
                'fuzzy_duplicates': self.stats['fuzzy_duplicates'],
                'validation_errors': self.stats['validation_errors'],
                'duplication_rate': (self.stats['duplicates_found'] / 
                                    max(self.stats['total_processed'], 1) * 100)
            }
        }
    
    def print_report(self, report: Dict):
        """Print formatted report."""
        print("\n" + "="*70)
        print("DUPLICATE CHECK REPORT")
        print("="*70)
        
        print("\n📊 STATISTICS:")
        print(f"  Total Questions: {report['summary']['total_questions']}")
        print(f"  Valid & Unique: {report['summary']['unique_questions']}")
        print(f"  Total Duplicates: {report['summary']['duplicates_found']}")
        print(f"    - Hash duplicates (exact): {report['summary']['hash_duplicates']}")
        print(f"    - Fuzzy duplicates (similar): {report['summary']['fuzzy_duplicates']}")
        print(f"  Validation Errors: {report['summary']['validation_errors']}")
        print(f"  Duplication Rate: {report['summary']['duplication_rate']:.1f}%")
        
        # Show validation errors
        if self.validation_errors:
            print(f"\n⚠️  VALIDATION ERRORS (showing first {min(10, len(self.validation_errors))}):")
            for i, error in enumerate(self.validation_errors[:10], 1):
                print(f"  {i}. Row {error['row']}: {error['error']}")
                print(f"     Question: {error['question']}...")
        
        # Ensure duplicates is a dict
        duplicates = report.get('duplicates', {})
        if not isinstance(duplicates, dict):
            duplicates = {'hash_duplicates': [], 'fuzzy_duplicates': []}
        
        # Show hash duplicates
        hash_dups = duplicates.get('hash_duplicates', [])
        if hash_dups:
            print(f"\n🔑 HASH DUPLICATES (exact matches, first {min(10, len(hash_dups))}):")
            for i, dup in enumerate(hash_dups[:10], 1):
                print(f"  {i}. Row {dup['row']} [{dup['level']}]: {dup['question']}...")
                if 'duplicate_of_row' in dup:
                    print(f"     Duplicate of row {dup['duplicate_of_row']}")
        
        # Show fuzzy duplicates
        fuzzy_dups = duplicates.get('fuzzy_duplicates', [])
        if fuzzy_dups:
            print(f"\n🔍 FUZZY DUPLICATES (similar, first {min(10, len(fuzzy_dups))}):")
            for i, dup in enumerate(fuzzy_dups[:10], 1):
                print(f"  {i}. Row {dup['row']} [{dup['level']}]: {dup['question']}...")
                print(f"     Similar to row {dup['similar_to_row']}: {dup['similar_to']}...")
        
        print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description='Italian Question Duplicate Checker (Updated for Database v1.3)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s check Italian_A1.csv                  # Check for duplicates
  %(prog)s dedupe Italian_A1.csv clean_A1.csv    # Remove duplicates
  %(prog)s merge A1_part1.csv A1_part2.csv A1_full.csv  # Merge and deduplicate
  %(prog)s validate Italian_A1.csv --check-topics  # Validate schema & topics
  
  %(prog)s check Italian_A1.csv --level A1 --verbose
  %(prog)s dedupe Italian_A1.csv clean.csv --threshold 0.9
        """
    )
    
    parser.add_argument('command', choices=['check', 'dedupe', 'merge', 'validate'],
                       help='Operation to perform')
    parser.add_argument('input', help='Input CSV file')
    parser.add_argument('input2', nargs='?', help='Second input file (for merge)')
    parser.add_argument('output', nargs='?', help='Output CSV file')
    parser.add_argument('--level', choices=CEFR_LEVELS,
                       help='Specify CEFR level (auto-detected from filename if not provided)')
    parser.add_argument('--threshold', type=float, default=0.85,
                       help='Similarity threshold for fuzzy matching (0.0-1.0, default 0.85)')
    parser.add_argument('--check-topics', action='store_true',
                       help='Validate topics against official list (requires make_italian_datasets.py)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress')
    parser.add_argument('--report', help='Save detailed report to file')
    
    args = parser.parse_args()
    
    checker = ItalianDuplicateChecker(
        verbose=args.verbose,
        level=args.level,
        check_topics=args.check_topics
    )
    
    if args.command == 'check':
        print(f"Checking {args.input} for duplicates...")
        report = checker.check_duplicates(args.input, args.threshold)
        checker.print_report(report)
    
    elif args.command == 'dedupe':
        if not args.output:
            base = Path(args.input).stem
            args.output = f"{base}_deduped.csv"
            print(f"No output file specified. Using: {args.output}")
        
        report = checker.deduplicate(args.input, args.output, args.threshold)
        if report:
            checker.print_report(report)
    
    elif args.command == 'merge':
        if not args.input2 or not args.output:
            print("❌ Merge command requires: merge file1.csv file2.csv output.csv")
            sys.exit(1)
        
        report = checker.merge_and_deduplicate(args.input, args.input2, args.output, args.threshold)
        if report:
            checker.print_report(report)
    
    elif args.command == 'validate':
        print(f"Validating {args.input}...")
        report = checker.validate_csv_schema(args.input)
        if report:
            checker.print_report(report)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Italian Duplicate Checker v1.3 - Updated for Database Compatibility\n")
        print("Usage examples:")
        print("  python duplicate_check.py check Italian_A1.csv")
        print("  python duplicate_check.py dedupe Italian_A1.csv clean_A1.csv")
        print("  python duplicate_check.py validate Italian_A1.csv --check-topics")
        print("\nFor full help: python duplicate_check.py --help")
    else:
        main()