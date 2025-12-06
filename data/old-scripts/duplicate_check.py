#!/usr/bin/env python3
"""
Italian Question Duplicate Checker
Simplified script for finding and removing duplicate questions from Italian language CSVs.

Usage:
    python italian_duplicate_checker.py check input.csv
    python italian_duplicate_checker.py dedupe input.csv output.csv --threshold 0.85
    python italian_duplicate_checker.py merge file1.csv file2.csv output.csv

Requirements:
    pip install pandas
"""

import csv
import argparse
import sys
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

# For pandas - will give helpful error if not installed
try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas")
    sys.exit(1)


class ItalianDuplicateChecker:
    """
    Checks for and removes duplicate Italian questions from CSVs.
    """
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.duplicates = []
        self.stats = {
            'total_processed': 0,
            'unique_questions': 0,
            'duplicates_found': 0,
            'duplicates_removed': 0
        }
    
    def check_duplicates(self, input_file: str, threshold: float = 0.85) -> Dict:
        """Check CSV for duplicates and generate report."""
        
        # Read input CSV with multiple encoding attempts
        df = self.read_csv_robust(input_file)
        
        if df is None:
            print(f"❌ Failed to read {input_file}")
            return self.generate_report()
        
        print(f"Checking {len(df)} questions for duplicates (threshold: {threshold:.0%})...")
        
        # Group questions to find duplicates
        question_groups = []
        
        for idx, row in df.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df)} questions...")
            
            # Extract key fields
            question_data = self.extract_question_data(row, idx)
            
            # Check if duplicate of any existing group
            found_group = False
            for group in question_groups:
                if self.is_duplicate_of_group(question_data, group, threshold):
                    group['duplicates'].append(question_data)
                    self.stats['duplicates_found'] += 1
                    found_group = True
                    break
            
            if not found_group:
                # Create new group
                question_groups.append({
                    'original': question_data,
                    'duplicates': []
                })
                self.stats['unique_questions'] += 1
        
        # Generate duplicate report
        self.generate_duplicate_report(question_groups)
        
        return self.generate_report()
    
    def deduplicate(self, input_file: str, output_file: str, threshold: float = 0.85) -> Dict:
        """Remove duplicates from CSV and save clean version."""
        
        # Read input CSV
        df = self.read_csv_robust(input_file)
        
        if df is None:
            print(f"❌ Failed to read {input_file}")
            return None
        
        print(f"Deduplicating {len(df)} questions (threshold: {threshold:.0%})...")
        
        # Track unique questions
        unique_questions = []
        unique_indices = []
        
        for idx, row in df.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df)} questions...")
            
            # Extract question data
            question_data = self.extract_question_data(row, idx)
            
            # Check if duplicate
            is_duplicate = False
            for unique_q in unique_questions:
                if self.is_duplicate(question_data, unique_q, threshold):
                    is_duplicate = True
                    self.stats['duplicates_found'] += 1
                    self.duplicates.append({
                        'index': idx,
                        'question': question_data['question_text'],
                        'duplicate_of': unique_q['question_text']
                    })
                    break
            
            if not is_duplicate:
                unique_questions.append(question_data)
                unique_indices.append(idx)
                self.stats['unique_questions'] += 1
        
        # Save deduplicated data
        df_clean = df.loc[unique_indices]
        self.stats['duplicates_removed'] = self.stats['duplicates_found']
        
        # Write to output
        try:
            df_clean.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✅ Saved {len(df_clean)} unique questions to {output_file}")
            print(f"🗑️  Removed {self.stats['duplicates_removed']} duplicates")
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
        
        # Now deduplicate
        unique_questions = []
        unique_indices = []
        
        for idx, row in df_combined.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df_combined)} questions...")
            
            # Extract question data
            question_data = self.extract_question_data(row, idx)
            
            # Check if duplicate
            is_duplicate = False
            for unique_q in unique_questions:
                if self.is_duplicate(question_data, unique_q, threshold):
                    is_duplicate = True
                    self.stats['duplicates_found'] += 1
                    break
            
            if not is_duplicate:
                unique_questions.append(question_data)
                unique_indices.append(idx)
                self.stats['unique_questions'] += 1
        
        # Save merged and deduplicated data
        df_clean = df_combined.loc[unique_indices]
        self.stats['duplicates_removed'] = self.stats['duplicates_found']
        
        # Write to output
        try:
            df_clean.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✅ Saved {len(df_clean)} unique questions to {output_file}")
            print(f"🗑️  Removed {self.stats['duplicates_removed']} duplicates during merge")
        except Exception as e:
            print(f"❌ Error saving file: {e}")
        
        return self.generate_report()
    
    def read_csv_robust(self, input_file: str) -> Optional[pd.DataFrame]:
        """Read CSV with multiple encoding attempts."""
        
        # Try different parsing strategies
        strategies = [
            {'encoding': 'utf-8'},
            {'encoding': 'utf-8', 'on_bad_lines': 'skip'},
            {'encoding': 'latin-1'},
            {'encoding': 'latin-1', 'on_bad_lines': 'skip'},
            {'encoding': 'utf-8', 'quoting': csv.QUOTE_ALL},
        ]
        
        for strategy in strategies:
            try:
                df = pd.read_csv(input_file, **strategy)
                if 'on_bad_lines' in strategy:
                    # Check if lines were skipped
                    with open(input_file, 'r', encoding=strategy.get('encoding', 'utf-8')) as f:
                        total_lines = sum(1 for line in f) - 1  # Subtract header
                    if len(df) < total_lines:
                        print(f"⚠️  Warning: Skipped {total_lines - len(df)} malformed lines")
                return df
            except Exception as e:
                continue
        
        return None
    
    def extract_question_data(self, row: pd.Series, idx: int) -> Dict:
        """Extract relevant fields from a question row."""
        
        # Handle potential field name variations
        field_mappings = {
            'question_text': ['question_text', 'question', 'Question'],
            'complete_sentence': ['complete_sentence', 'complete', 'full'],
            'english_translation': ['english_translation', 'translation', 'English'],
            'topic': ['topic', 'Topic', 'category'],
            'cefr_level': ['cefr_level', 'level', 'Level', 'CEFR']
        }
        
        data = {'index': idx}
        
        for standard_field, variations in field_mappings.items():
            value = ''
            for variant in variations:
                if variant in row.index:
                    value = str(row[variant]) if pd.notna(row[variant]) else ''
                    break
            data[standard_field] = value.strip()
        
        return data
    
    def is_duplicate(self, q1: Dict, q2: Dict, threshold: float) -> bool:
        """Check if two questions are duplicates."""
        
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
        
        # Check if question AND translation are very similar (both > 80%)
        if q1['question_text'] and q2['question_text'] and q1['english_translation'] and q2['english_translation']:
            text_sim = self.similarity_ratio(q1['question_text'], q2['question_text'])
            trans_sim = self.similarity_ratio(q1['english_translation'], q2['english_translation'])
            if text_sim > 0.8 and trans_sim > 0.8:
                return True
        
        return False
    
    def is_duplicate_of_group(self, question: Dict, group: Dict, threshold: float) -> bool:
        """Check if question is duplicate of a group's original question."""
        return self.is_duplicate(question, group['original'], threshold)
    
    def similarity_ratio(self, str1: str, str2: str) -> float:
        """Calculate string similarity ratio."""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def generate_duplicate_report(self, question_groups: List[Dict]):
        """Generate detailed duplicate report."""
        
        for group in question_groups:
            if group['duplicates']:
                original = group['original']
                self.duplicates.append({
                    'original_index': original['index'],
                    'original_text': original['question_text'],
                    'duplicate_count': len(group['duplicates']),
                    'duplicate_indices': [d['index'] for d in group['duplicates']]
                })
    
    def generate_report(self) -> Dict:
        """Generate analysis report."""
        return {
            'statistics': self.stats,
            'duplicates': self.duplicates[:20],  # First 20 duplicate groups
            'summary': {
                'total_questions': self.stats['total_processed'],
                'unique_questions': self.stats['unique_questions'],
                'duplicates_found': self.stats['duplicates_found'],
                'duplication_rate': (self.stats['duplicates_found'] / 
                                    max(self.stats['total_processed'], 1) * 100)
            }
        }
    
    def print_report(self, report: Dict):
        """Print formatted report."""
        print("\n" + "="*60)
        print("DUPLICATE CHECK REPORT")
        print("="*60)
        
        print("\n📊 STATISTICS:")
        print(f"  Total Questions: {report['summary']['total_questions']}")
        print(f"  Unique Questions: {report['summary']['unique_questions']}")
        print(f"  Duplicates Found: {report['summary']['duplicates_found']}")
        print(f"  Duplication Rate: {report['summary']['duplication_rate']:.1f}%")
        
        if report['duplicates']:
            print(f"\n🔄 DUPLICATE GROUPS (showing first {len(report['duplicates'])}):")
            for i, dup_group in enumerate(report['duplicates'][:10], 1):
                print(f"\n  Group {i}:")
                print(f"    Original (row {dup_group.get('original_index', 'N/A')}): {dup_group.get('original_text', dup_group.get('question', 'N/A'))[:60]}...")
                if 'duplicate_count' in dup_group:
                    print(f"    {dup_group['duplicate_count']} duplicate(s) at rows: {dup_group['duplicate_indices'][:5]}")
                    if len(dup_group['duplicate_indices']) > 5:
                        print(f"      ... and {len(dup_group['duplicate_indices']) - 5} more")
                elif 'duplicate_of' in dup_group:
                    print(f"    Duplicate of: {dup_group['duplicate_of'][:60]}...")
        
        print("\n" + "="*60)
    
    def save_detailed_report(self, report: Dict, filename: str = "duplicate_report.txt"):
        """Save detailed report to file."""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("COMPLETE DUPLICATE ANALYSIS REPORT\n")
            f.write("="*60 + "\n\n")
            
            f.write("SUMMARY:\n")
            f.write(f"  Total Questions: {report['summary']['total_questions']}\n")
            f.write(f"  Unique Questions: {report['summary']['unique_questions']}\n")
            f.write(f"  Duplicates Found: {report['summary']['duplicates_found']}\n")
            f.write(f"  Duplication Rate: {report['summary']['duplication_rate']:.1f}%\n")
            
            if self.duplicates:
                f.write(f"\n\nALL DUPLICATE GROUPS ({len(self.duplicates)} groups):\n")
                f.write("="*60 + "\n")
                
                for i, dup_group in enumerate(self.duplicates, 1):
                    f.write(f"\nGroup {i}:\n")
                    f.write(f"  Original (row {dup_group.get('original_index', 'N/A')}): {dup_group.get('original_text', 'N/A')}\n")
                    
                    if 'duplicate_count' in dup_group:
                        f.write(f"  Duplicates ({dup_group['duplicate_count']}): rows {dup_group['duplicate_indices']}\n")
                    elif 'question' in dup_group:
                        f.write(f"  Question: {dup_group['question']}\n")
                        f.write(f"  Duplicate of: {dup_group.get('duplicate_of', 'N/A')}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Italian Question Duplicate Checker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s check input.csv                    # Check for duplicates
  %(prog)s dedupe input.csv output.csv        # Remove duplicates
  %(prog)s merge file1.csv file2.csv output.csv  # Merge and deduplicate
  
  %(prog)s check input.csv --threshold 0.9    # Stricter duplicate detection
  %(prog)s dedupe input.csv output.csv --verbose  # Show progress
        """
    )
    
    parser.add_argument('command', choices=['check', 'dedupe', 'merge'],
                       help='Operation to perform')
    parser.add_argument('input', help='Input CSV file(s)')
    parser.add_argument('input2', nargs='?', help='Second input file (for merge)')
    parser.add_argument('output', nargs='?', help='Output CSV file')
    parser.add_argument('--threshold', type=float, default=0.85,
                       help='Similarity threshold for duplicates (0.0-1.0, default 0.85)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress')
    parser.add_argument('--report', help='Save detailed report to file')
    
    args = parser.parse_args()
    
    checker = ItalianDuplicateChecker(verbose=args.verbose)
    
    if args.command == 'check':
        print(f"Checking {args.input} for duplicates...")
        report = checker.check_duplicates(args.input, args.threshold)
        checker.print_report(report)
        
        if args.report:
            checker.save_detailed_report(report, args.report)
            print(f"\n📄 Detailed report saved to {args.report}")
    
    elif args.command == 'dedupe':
        if not args.output:
            # Auto-generate output filename if not provided
            base = Path(args.input).stem
            args.output = f"{base}_deduped.csv"
            print(f"No output file specified. Using: {args.output}")
        
        report = checker.deduplicate(args.input, args.output, args.threshold)
        if report:
            checker.print_report(report)
            
            if args.report:
                checker.save_detailed_report(report, args.report)
                print(f"\n📄 Detailed report saved to {args.report}")
    
    elif args.command == 'merge':
        if not args.input2 or not args.output:
            print("❌ Merge command requires: merge file1.csv file2.csv output.csv")
            sys.exit(1)
        
        report = checker.merge_and_deduplicate(args.input, args.input2, args.output, args.threshold)
        if report:
            checker.print_report(report)
            
            if args.report:
                checker.save_detailed_report(report, args.report)
                print(f"\n📄 Detailed report saved to {args.report}")


if __name__ == "__main__":
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        print("Italian Duplicate Checker - Quick Start\n")
        print("Usage examples:")
        print("  python italian_duplicate_checker.py check your_questions.csv")
        print("  python italian_duplicate_checker.py dedupe input.csv output.csv")
        print("  python italian_duplicate_checker.py merge A1.csv A2.csv combined.csv")
        print("\nFor full help: python italian_duplicate_checker.py --help")
    else:
        main()