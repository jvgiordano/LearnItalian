#!/usr/bin/env python3
"""
Migrate hints from question_text (in parentheses) to the hint column in CSV files.
This is a focused tool specifically for hint migration.

Usage:
    python migrate_hints.py input.csv output.csv
    python migrate_hints.py input.csv output.csv --check-only
    python migrate_hints.py *.csv --batch
"""

import csv
import re
import sys
import os
from pathlib import Path
import argparse
from typing import Dict, List, Tuple

class HintMigrator:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.stats = {
            'total_questions': 0,
            'hints_migrated': 0,
            'hints_already_present': 0,
            'no_hint_found': 0,
            'errors': 0
        }
        self.migrated_hints = []
        
    def extract_hint_from_question(self, question_text: str) -> Tuple[str, str]:
        """
        Extract hint from question text if it appears in parentheses at the end.
        Returns: (cleaned_question_text, extracted_hint)
        """
        if not question_text:
            return question_text, ""
        
        # Pattern to match parentheses at the end of the question
        # This handles multiple formats:
        # 1. Simple: "Question text (hint)"
        # 2. With punctuation: "Question text. (hint)"
        # 3. With spaces: "Question text (hint) "
        
        # First, check if there's a parenthetical at the end
        pattern = r'\s*\(([^)]+)\)\s*$'
        match = re.search(pattern, question_text)
        
        if match:
            hint = match.group(1).strip()
            # Remove the hint from the question text
            cleaned_text = question_text[:match.start()].rstrip()
            
            # Clean up any trailing punctuation before the parentheses
            cleaned_text = cleaned_text.rstrip('.,;:')
            
            return cleaned_text, hint
        
        return question_text, ""
    
    def process_question(self, row: Dict) -> Dict:
        """Process a single question row to migrate hints."""
        self.stats['total_questions'] += 1
        
        # Get current values
        question_text = row.get('question_text', '')
        current_hint = row.get('hint', '')
        
        # If hint already exists, don't overwrite it
        if current_hint and current_hint.strip():
            self.stats['hints_already_present'] += 1
            if self.verbose:
                print(f"  Hint already present: '{current_hint}'")
            return row
        
        # Try to extract hint from question text
        cleaned_text, extracted_hint = self.extract_hint_from_question(question_text)
        
        if extracted_hint:
            # Update the row
            row['question_text'] = cleaned_text
            row['hint'] = extracted_hint
            
            self.stats['hints_migrated'] += 1
            self.migrated_hints.append({
                'original': question_text,
                'cleaned': cleaned_text,
                'hint': extracted_hint
            })
            
            if self.verbose:
                print(f"  Migrated hint: '{extracted_hint}'")
                print(f"    From: {question_text[:60]}...")
                print(f"    To:   {cleaned_text[:60]}...")
        else:
            self.stats['no_hint_found'] += 1
        
        return row
    
    def migrate_csv(self, input_file: str, output_file: str = None, check_only: bool = False) -> bool:
        """
        Migrate hints in a CSV file.
        If check_only is True, only report what would be done without modifying.
        """
        print(f"\nProcessing: {input_file}")
        
        if not os.path.exists(input_file):
            print(f"  ❌ File not found: {input_file}")
            return False
        
        # Reset stats for this file
        self.stats = {k: 0 for k in self.stats}
        self.migrated_hints = []
        
        try:
            # Read the CSV
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                # Check if hint column exists
                if 'hint' not in fieldnames:
                    print(f"  ❌ No 'hint' column found in CSV")
                    return False
                
                rows = []
                for row in reader:
                    processed_row = self.process_question(row)
                    rows.append(processed_row)
            
            # Report results
            self.print_stats()
            
            # Write output if not check-only mode
            if not check_only and output_file and self.stats['hints_migrated'] > 0:
                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"  ✅ Wrote updated CSV to: {output_file}")
                return True
            elif check_only:
                print(f"  ℹ️  Check-only mode - no files modified")
                if self.stats['hints_migrated'] > 0:
                    print(f"  💡 Would migrate {self.stats['hints_migrated']} hints")
                return True
            elif self.stats['hints_migrated'] == 0:
                print(f"  ℹ️  No hints to migrate - file unchanged")
                return True
                
        except Exception as e:
            print(f"  ❌ Error processing file: {e}")
            self.stats['errors'] += 1
            return False
    
    def print_stats(self):
        """Print migration statistics."""
        print(f"\n  📊 Statistics:")
        print(f"     Total questions: {self.stats['total_questions']}")
        print(f"     Hints migrated: {self.stats['hints_migrated']}")
        print(f"     Hints already present: {self.stats['hints_already_present']}")
        print(f"     No hint found: {self.stats['no_hint_found']}")
        
        if self.verbose and self.migrated_hints:
            print(f"\n  📝 Sample migrations (first 5):")
            for i, migration in enumerate(self.migrated_hints[:5], 1):
                print(f"\n  {i}. Hint: '{migration['hint']}'")
                print(f"     Original: {migration['original'][:80]}...")
                print(f"     Cleaned:  {migration['cleaned'][:80]}...")
    
    def show_examples(self, input_file: str, limit: int = 10):
        """Show examples of hints that would be migrated."""
        print(f"\nScanning for hints in: {input_file}")
        
        examples = []
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    question_text = row.get('question_text', '')
                    current_hint = row.get('hint', '')
                    
                    # Skip if hint already exists
                    if current_hint and current_hint.strip():
                        continue
                    
                    # Check for hint in parentheses
                    if '(' in question_text and question_text.rstrip().endswith(')'):
                        cleaned, hint = self.extract_hint_from_question(question_text)
                        if hint:
                            examples.append({
                                'question': question_text,
                                'cleaned': cleaned,
                                'hint': hint,
                                'level': row.get('cefr_level', 'Unknown'),
                                'topic': row.get('topic', 'Unknown')
                            })
                            
                            if len(examples) >= limit:
                                break
            
            if examples:
                print(f"\n  Found {len(examples)} questions with hints to migrate:")
                for i, ex in enumerate(examples, 1):
                    print(f"\n  {i}. [{ex['level']}] {ex['topic']}")
                    print(f"     Current: {ex['question'][:80]}...")
                    print(f"     Would extract hint: '{ex['hint']}'")
                    print(f"     Cleaned question: {ex['cleaned'][:80]}...")
            else:
                print("  ✅ No hints found in parentheses to migrate")
                
        except Exception as e:
            print(f"  ❌ Error scanning file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Migrate hints from question text to hint column in CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.csv output.csv              # Migrate hints to new file
  %(prog)s input.csv output.csv --check-only # Preview what would be migrated
  %(prog)s input.csv --in-place              # Modify file in place
  %(prog)s input.csv --examples              # Show examples of hints to migrate
  
Batch processing:
  %(prog)s data/*.csv --batch                # Process multiple files
  %(prog)s data/*.csv --batch --check-only   # Check multiple files
        """
    )
    
    parser.add_argument('input', help='Input CSV file(s)', nargs='+')
    parser.add_argument('output', nargs='?', help='Output CSV file')
    parser.add_argument('--check-only', action='store_true',
                       help='Check what would be migrated without modifying files')
    parser.add_argument('--in-place', action='store_true',
                       help='Modify input file in place')
    parser.add_argument('--batch', action='store_true',
                       help='Process multiple files')
    parser.add_argument('--examples', action='store_true',
                       help='Show examples of hints that would be migrated')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress')
    
    args = parser.parse_args()
    
    migrator = HintMigrator(verbose=args.verbose)
    
    # Handle examples mode
    if args.examples:
        for input_file in args.input:
            if os.path.exists(input_file):
                migrator.show_examples(input_file)
        return
    
    # Handle batch mode
    if args.batch or len(args.input) > 1:
        print(f"Batch processing {len(args.input)} files...")
        
        total_migrated = 0
        files_modified = 0
        
        for input_file in args.input:
            if not os.path.exists(input_file):
                print(f"  ⚠️  Skipping non-existent file: {input_file}")
                continue
            
            # Determine output file
            if args.in_place:
                output_file = input_file
            else:
                # Create output filename with _migrated suffix
                path = Path(input_file)
                output_file = path.parent / f"{path.stem}_migrated{path.suffix}"
            
            # Process the file
            if migrator.migrate_csv(input_file, str(output_file), args.check_only):
                if migrator.stats['hints_migrated'] > 0:
                    total_migrated += migrator.stats['hints_migrated']
                    files_modified += 1
        
        print(f"\n{'='*60}")
        print(f"BATCH PROCESSING COMPLETE")
        print(f"Files processed: {len(args.input)}")
        print(f"Files with migrations: {files_modified}")
        print(f"Total hints migrated: {total_migrated}")
        print(f"{'='*60}")
    
    else:
        # Single file mode
        input_file = args.input[0]
        
        # Determine output file
        if args.in_place:
            output_file = input_file
        elif args.output:
            output_file = args.output
        else:
            # Default: create with _migrated suffix
            path = Path(input_file)
            output_file = path.parent / f"{path.stem}_migrated{path.suffix}"
        
        # Process the file
        success = migrator.migrate_csv(input_file, str(output_file), args.check_only)
        
        if success and not args.check_only:
            print(f"\n✅ Migration complete!")
            if migrator.stats['hints_migrated'] > 0:
                print(f"   Migrated {migrator.stats['hints_migrated']} hints")
                print(f"   Output: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Hint Migration Tool - Quick Start\n")
        print("This tool migrates hints from parentheses in question_text to the hint column.\n")
        print("Usage examples:")
        print("  python migrate_hints.py input.csv output.csv")
        print("  python migrate_hints.py input.csv --in-place")
        print("  python migrate_hints.py input.csv --examples")
        print("  python migrate_hints.py data/*.csv --batch")
        print("\nFor full help: python migrate_hints.py --help")
    else:
        main()