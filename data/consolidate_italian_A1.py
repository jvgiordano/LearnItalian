import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import csv

def diagnose_csv_issues(input_file='Italian_A1.csv'):
    """
    Diagnose CSV file issues before processing.
    """
    print(f"Diagnosing {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as file:
        # Read first line to get expected number of columns
        first_line = file.readline()
        expected_cols = len(first_line.split(','))
        
        # Check for common issues
        file.seek(0)
        reader = csv.reader(file)
        header = next(reader)
        print(f"Headers ({len(header)} columns): {header[:5]}...")  # Show first 5 headers
        
        problematic_lines = []
        for line_num, row in enumerate(reader, start=2):
            if len(row) != len(header):
                problematic_lines.append((line_num, len(row), row[:3]))  # Store line number, field count, first 3 fields
                if len(problematic_lines) >= 10:  # Limit to first 10 issues
                    break
        
        if problematic_lines:
            print(f"\nFound {len(problematic_lines)} problematic lines:")
            for line_num, field_count, sample in problematic_lines[:5]:
                print(f"  Line {line_num}: Expected {len(header)} fields, got {field_count}")
            print("\nAttempting to fix by using proper CSV quoting...")
            return True
        else:
            print("No obvious issues found.")
            return False

def consolidate_italian_topics(input_file='Italian_A1.csv', output_file='Italian_A1_consolidated.csv', report_file='consolidation_report.txt'):
    """
    Consolidate Italian grammar topics according to specified rules.
    """
    
    # First diagnose potential issues
    has_issues = diagnose_csv_issues(input_file)
    
    # Read the CSV file with appropriate handling
    print(f"\nReading {input_file}...")
    
    try:
        if has_issues:
            # Try different parsing strategies for problematic files
            try:
                # Most robust: let pandas infer the delimiter and handle quoting
                df = pd.read_csv(input_file, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            except:
                try:
                    # Try with Python engine which is more flexible
                    df = pd.read_csv(input_file, engine='python', quoting=csv.QUOTE_ALL)
                except:
                    # Skip bad lines as last resort
                    df = pd.read_csv(input_file, on_bad_lines='skip', engine='python')
                    print("Warning: Some lines were skipped due to formatting issues.")
        else:
            # Standard reading for clean files
            df = pd.read_csv(input_file)
            
    except Exception as e:
        print(f"Error reading CSV: {e}")
        print("\nTrying alternative reading method with line-by-line processing...")
        
        # Manual line-by-line reading as fallback
        rows = []
        with open(input_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for i, row in enumerate(csv_reader):
                try:
                    rows.append(row)
                except Exception as row_error:
                    print(f"Skipping row {i+1}: {row_error}")
        
        df = pd.DataFrame(rows)
    
    print(f"Successfully loaded {len(df)} questions")
    
    # Create a copy for modifications
    df_consolidated = df.copy()
    
    # Initialize report
    report = []
    report.append(f"Italian A1 Topic Consolidation Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 60)
    report.append("")
    
    # Track changes
    changes = defaultdict(list)
    topic_counts_before = df['topic'].value_counts().to_dict()
    
    # Define topic mappings
    topic_mappings = {
        # Vocabulary topics (add "vocabolario")
        'Animali': 'Animali (vocabolario)',
        'Animali (comuni)': 'Animali (vocabolario)',
        'Casa': 'Casa (vocabolario)',
        'Casa (stanze e oggetti)': 'Casa (vocabolario)',
        'Oggetti': 'Casa (vocabolario)',  # Special merge
        'Corpo': 'Corpo (vocabolario)',
        'Corpo (base)': 'Corpo (vocabolario)',
        'Cibo': 'Cibo e bevande (vocabolario)',
        'Cibo e bevande (base)': 'Cibo e bevande (vocabolario)',
        'Abbigliamento': 'Abbigliamento (vocabolario)',
        'Colori': 'Colori (vocabolario)',
        'Famiglia': 'Famiglia (vocabolario)',
        'Professioni': 'Professioni (vocabolario)',
        'Professioni (base)': 'Professioni (vocabolario)',
        'Trasporti': 'Trasporti (vocabolario)',
        'Trasporti (base)': 'Trasporti (vocabolario)',
        
        # Expression/Communication topics
        'Saluti': 'Saluti e presentazioni',
        'Saluti, presentazioni, tu/Lei': 'Saluti e presentazioni',
        'Espressioni': 'Espressioni comuni',
        'Espressioni comuni': 'Espressioni comuni',
        'Ristorante': 'Ristorante (frasi e vocabolario)',
        'Ristorante (frasi base)': 'Ristorante (frasi e vocabolario)',
        'Routine Quotidiana': 'Routine quotidiana',
        'Routine quotidiana': 'Routine quotidiana',
        
        # Grammar topics
        'Articoli Partitivi': 'Articoli partitivi',
        'Aggettivi': 'Aggettivi (accordo e uso)',
        'Aggettivi: accordo base': 'Aggettivi (accordo e uso)',
        'Aggettivi Possessivi': 'Aggettivi possessivi',
        'Aggettivi possessivi (base)': 'Aggettivi possessivi',
        'Parole Interrogative': 'Parole interrogative',
        'Parole interrogative': 'Parole interrogative',
        'Preposizioni': 'Preposizioni semplici',
        'Preposizioni semplici (uso base)': 'Preposizioni semplici',
        'Piacere': 'Piacere (uso base: piace/piacciono)',
        'Piacere (uso base: piace/piacciono)': 'Piacere (uso base: piace/piacciono)',
        'Dimostrativi (base)': 'Dimostrativi',
        'Pronomi soggetto': 'Pronomi soggetto',
        'Nomi: genere e numero': 'Nomi (genere e numero)',
        'Forme negative semplici': 'Forme negative semplici',
        'C\'è / Ci sono': 'C\'è / Ci sono',
        'Si impersonale': 'Si impersonale',
        
        # Verb topics
        'Verbo Avere': 'Verbo avere (to have)',
        'Verbo avere (to have)': 'Verbo avere (to have)',
        'Verbo Essere': 'Verbo essere (to be)',
        'Verbo essere (to be)': 'Verbo essere (to be)',
        'Verbi Riflessivi': 'Verbi riflessivi',
        'Verbi Irregolari': 'Verbi irregolari',
        'Verbi modali (uso base: potere, dovere, volere)': 'Verbi modali (potere, dovere, volere)',
        
        # Time/Date topics
        'Giorni della settimana': 'Giorni della settimana (vocabolario)',
        'Mesi': 'Mesi (vocabolario)',
        'Stagioni': 'Stagioni (vocabolario)',
        'Orario': 'Orario (telling time)',
        'Orario (telling time)': 'Orario (telling time)',
        'Espressioni di tempo basilari': 'Espressioni di tempo',
        'Tempo': 'Meteo (vocabolario)',
        'Meteo (base)': 'Meteo (vocabolario)',
        
        # Numbers/Money
        'Numeri': 'Numeri e prezzi (0-100)',
        'Numeri 0–100 e prezzi': 'Numeri e prezzi (0-100)',
        'Denaro': 'Numeri e prezzi (0-100)',
        
        # Location/Geography
        'Geografia': 'Geografia (paesi, città, nazionalità)',
        'Geografia: paesi, città, nazionalità': 'Geografia (paesi, città, nazionalità)',
        'Nazionalità': 'Geografia (paesi, città, nazionalità)',
        'Direzioni e luoghi in città': 'Direzioni e luoghi in città',
        'Luoghi': 'Direzioni e luoghi in città',
        
        # Miscellaneous
        'Cultura Generale': 'Cultura generale',
        
        # Presente indicativo variations (keep as is for now)
        'Presente indicativo: verbi regolari -are': 'Presente indicativo: verbi regolari -are',
        'Presente indicativo: verbi regolari -ere': 'Presente indicativo: verbi regolari -ere',
        'Presente indicativo: verbi regolari -ire': 'Presente indicativo: verbi regolari -ire',
        'Presente indicativo: verbi regolari -ire con -isc- (tipo finire)': 'Presente indicativo: verbi regolari -ire con -isc-',
    }
    
    # Apply basic mappings first
    for idx, row in df_consolidated.iterrows():
        old_topic = row['topic']
        new_topic = topic_mappings.get(old_topic, old_topic)
        
        if old_topic != new_topic:
            df_consolidated.at[idx, 'topic'] = new_topic
            changes[f"{old_topic} → {new_topic}"].append(idx)
    
    # Special handling for "Articoli" and "Articoli determinativi e indeterminativi"
    articoli_determinativi = ['il', 'la', 'lo', 'l\'', 'gli', 'le', 'i']
    articoli_indeterminativi = ['un', 'uno', 'una', 'un\'']
    
    def classify_articolo(row):
        """Classify article based on correct answer."""
        try:
            correct_option_letter = row.get('correct_option', '')
            if pd.isna(correct_option_letter) or correct_option_letter == '':
                return 'Articoli (generale)'
            
            # Get the correct answer from the option column
            option_col = f'option_{correct_option_letter.lower()}'
            if option_col in row.index:
                correct_answer = str(row[option_col]).lower().strip()
                
                # Check if it's determinativo or indeterminativo
                if correct_answer in articoli_determinativi:
                    return 'Articoli determinativi'
                elif correct_answer in articoli_indeterminativi:
                    return 'Articoli indeterminativi'
        except Exception as e:
            print(f"Error classifying articolo: {e}")
        
        return 'Articoli (generale)'
    
    # Process Articoli questions
    articoli_mask = df_consolidated['topic'].isin(['Articoli', 'Articoli determinativi e indeterminativi'])
    for idx in df_consolidated[articoli_mask].index:
        old_topic = df_consolidated.at[idx, 'topic']
        new_topic = classify_articolo(df_consolidated.loc[idx])
        df_consolidated.at[idx, 'topic'] = new_topic
        changes[f"{old_topic} → {new_topic}"].append(idx)
    
    # Special handling for "Presente Indicativo"
    def classify_presente_indicativo(row):
        """Classify presente indicativo based on explanation."""
        explanation = str(row.get('explanation', '')).lower()
        
        # Check for irregular verbs
        irregular_patterns = [
            'irregular', 'irregolare', 'essere', 'avere', 'andare', 'fare', 
            'dare', 'stare', 'sapere', 'dire', 'venire', 'uscire'
        ]
        for pattern in irregular_patterns:
            if pattern in explanation:
                return 'Verbi irregolari'
        
        # Check for -isc- verbs
        if '-isc-' in explanation or 'finire' in explanation or 'capire' in explanation:
            return 'Presente indicativo: verbi regolari -ire con -isc-'
        
        # Check for regular verb patterns
        if '-are verb' in explanation or 'of -are' in explanation or 'verbi -are' in explanation:
            return 'Presente indicativo: verbi regolari -are'
        elif '-ere verb' in explanation or 'of -ere' in explanation or 'verbi -ere' in explanation:
            return 'Presente indicativo: verbi regolari -ere'
        elif '-ire verb' in explanation or 'of -ire' in explanation or 'verbi -ire' in explanation:
            return 'Presente indicativo: verbi regolari -ire'
        
        # Try to find infinitive patterns (e.g., "parlare → parlo")
        infinitive_match = re.search(r'(\w+are|\w+ere|\w+ire)\s*[→-]+\s*\w+', explanation)
        if infinitive_match:
            infinitive = infinitive_match.group(1)
            if infinitive.endswith('are'):
                return 'Presente indicativo: verbi regolari -are'
            elif infinitive.endswith('ere'):
                return 'Presente indicativo: verbi regolari -ere'
            elif infinitive.endswith('ire'):
                return 'Presente indicativo: verbi regolari -ire'
        
        return 'Presente indicativo (generale)'
    
    # Process Presente Indicativo questions
    presente_mask = df_consolidated['topic'] == 'Presente Indicativo'
    for idx in df_consolidated[presente_mask].index:
        old_topic = df_consolidated.at[idx, 'topic']
        new_topic = classify_presente_indicativo(df_consolidated.loc[idx])
        df_consolidated.at[idx, 'topic'] = new_topic
        changes[f"{old_topic} → {new_topic}"].append(idx)
    
    # Remove Comparativi questions (as requested)
    comparativi_mask = df_consolidated['topic'] == 'Comparativi'
    if comparativi_mask.sum() > 0:
        df_consolidated = df_consolidated[~comparativi_mask]
        report.append(f"Removed {comparativi_mask.sum()} Comparativi questions as requested.")
        report.append("")
    
    # Generate detailed report
    topic_counts_after = df_consolidated['topic'].value_counts().to_dict()
    
    report.append("TOPIC CONSOLIDATION SUMMARY")
    report.append("-" * 40)
    
    # Group changes by transformation type
    for change_type, indices in sorted(changes.items()):
        if indices:
            report.append(f"{change_type}: {len(indices)} questions")
    
    report.append("")
    report.append("BEFORE AND AFTER COMPARISON")
    report.append("-" * 40)
    
    # Show topics that were consolidated
    all_topics = set(list(topic_counts_before.keys()) + list(topic_counts_after.keys()))
    consolidated_topics = []
    
    for topic in sorted(all_topics):
        before = topic_counts_before.get(topic, 0)
        after = topic_counts_after.get(topic, 0)
        
        if before != after or (before == 0 and after > 0):
            consolidated_topics.append(f"{topic}: {before} → {after} questions")
    
    for item in consolidated_topics:
        report.append(item)
    
    # Check for any unmapped topics
    unmapped = []
    for topic in topic_counts_after.keys():
        if topic not in topic_mappings.values() and topic not in [
            'Articoli determinativi', 'Articoli indeterminativi', 'Articoli (generale)',
            'Presente indicativo (generale)', 'Verbi irregolari'
        ] and 'Presente indicativo:' not in topic:
            unmapped.append(topic)
    
    if unmapped:
        report.append("")
        report.append("TOPICS LEFT AS-IS (REVIEW RECOMMENDED)")
        report.append("-" * 40)
        for topic in unmapped:
            count = topic_counts_after[topic]
            report.append(f"{topic}: {count} questions")
    
    # Final statistics
    report.append("")
    report.append("FINAL STATISTICS")
    report.append("-" * 40)
    report.append(f"Total questions before: {len(df)}")
    report.append(f"Total questions after: {len(df_consolidated)}")
    report.append(f"Number of unique topics before: {len(topic_counts_before)}")
    report.append(f"Number of unique topics after: {len(topic_counts_after)}")
    
    # Save consolidated CSV
    try:
        df_consolidated.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"\nConsolidated data saved to {output_file}")
    except Exception as e:
        print(f"Error saving CSV: {e}")
        # Try alternative saving method
        df_consolidated.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)
        print(f"Saved with full quoting to {output_file}")
    
    # Save report
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))
    print(f"Report saved to {report_file}")
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("CONSOLIDATION COMPLETE")
    print(f"Topics reduced from {len(topic_counts_before)} to {len(topic_counts_after)}")
    print(f"Check {report_file} for detailed changes")
    
    return df_consolidated

# Run the consolidation
if __name__ == "__main__":
    try:
        consolidated_df = consolidate_italian_topics()
        
        # Print final topic distribution
        print("\n" + "=" * 60)
        print("FINAL TOPIC DISTRIBUTION")
        print("-" * 40)
        
        topic_counts = consolidated_df['topic'].value_counts()
        for topic, count in topic_counts.items():
            print(f"{topic}: {count} questions")
        
        print("-" * 40)
        print(f"TOTAL: {len(consolidated_df)} questions")
        
    except Exception as e:
        print(f"\nFatal error: {e}")
        print("\nPlease check that:")
        print("1. The file 'Italian_A1.csv' exists in the current directory")
        print("2. The CSV file is properly formatted")
        print("3. You have write permissions in the current directory")
        
        import traceback
        print("\nFull error trace:")
        traceback.print_exc()