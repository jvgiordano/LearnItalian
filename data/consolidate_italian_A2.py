import pandas as pd
from collections import defaultdict
from datetime import datetime
import csv

def diagnose_csv_issues(input_file='Italian_A2.csv'):
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

def consolidate_italian_topics_a2(input_file='Italian_A2.csv', output_file='Italian_A2_consolidated.csv', report_file='consolidation_report_A2.txt'):
    """
    Consolidate Italian A2 grammar topics according to specified rules.
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
    report.append(f"Italian A2 Topic Consolidation Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 60)
    report.append("")
    
    # Track changes
    changes = defaultdict(list)
    topic_counts_before = df['topic'].value_counts().to_dict()
    
    # Define comprehensive topic mappings based on our agreed consolidation
    topic_mappings = {
        # Grammar Topics (merged to general headings)
        'Aggettivi dimostrativi': 'Aggettivi Dimostrativi',
        'Aggettivi e Pronomi Indefiniti': 'Aggettivi e Pronomi Indefiniti',
        'Aggettivi e pronomi indefiniti': 'Aggettivi e Pronomi Indefiniti',
        'Aggettivi possessivi': 'Aggettivi Possessivi',
        'Articoli partitivi': 'Articoli Partitivi',
        'Avverbi di frequenza/tempo/luogo': 'Avverbi di Frequenza/Tempo/Luogo',
        'Comparativi e superlativi': 'Comparativi e Superlativi',
        'Comparativi e superlativi (base)': 'Comparativi e Superlativi',
        'Condizionale Presente': 'Condizionale Presente',
        'Condizionale presente': 'Condizionale Presente',
        'Confronto articoli': 'Confronto Articoli',
        'Confronto articoli (def/indef/partitivi)': 'Confronto Articoli',
        'Espressioni di tempo (fa, da, tra/fra)': 'Espressioni di Tempo (fa, da, tra/fra)',
        'Espressioni di tempo (fa, da, tra/fra; ore)': 'Espressioni di Tempo (fa, da, tra/fra)',
        'Futuro semplice': 'Futuro Semplice',
        'Futuro semplice (base)': 'Futuro Semplice',
        'Imperativo (tu/noi/voi)': 'Imperativo (tu/noi/voi)',
        'Imperativo (tu/noi/voi; base)': 'Imperativo (tu/noi/voi)',
        'Imperativo Formale': 'Imperativo Formale',
        'Imperfetto': 'Imperfetto',
        'Imperfetto (introduzione)': 'Imperfetto',
        'Imperfetto vs Passato prossimo': 'Imperfetto vs Passato Prossimo',
        'Imperfetto vs. Passato Prossimo': 'Imperfetto vs Passato Prossimo',
        'Parole interrogative': 'Parole Interrogative',
        'Particella ci': 'Particella Ci',
        'Particella ci (base)': 'Particella Ci',
        'Particella ne': 'Particella Ne',
        'Particella ne (base)': 'Particella Ne',
        'Participio passato (comuni; irregolari frequenti)': 'Participio Passato (irregolari frequenti)',
        'Participio passato (irregolari frequenti)': 'Participio Passato (irregolari frequenti)',
        'Passato prossimo (essere/avere, accordo)': 'Passato Prossimo (essere/avere, accordo)',
        'Passato prossimo (reg/irr; essere/avere; accordo)': 'Participio Passato (irregolari frequenti)',
        'Piacere (con pronomi, passato)': 'Piacere (con pronomi, passato)',
        'Piacere (con pronomi; passato)': 'Piacere (con pronomi, passato)',
        'Preposizioni Semplici': 'Preposizioni Semplici',
        'Preposizioni semplici': 'Preposizioni Semplici',
        'Preposizioni articolate': 'Preposizioni Articolate',
        'Pronomi Combinati': 'Pronomi Combinati',
        'Pronomi Relativi': 'Pronomi Relativi',
        'Pronomi diretti': 'Pronomi Diretti',
        'Pronomi indiretti': 'Pronomi Indiretti',
        'Si Impersonale': 'Si Impersonale',
        'Stare + Gerundio': 'Stare + Gerundio',
        'Stare + gerundio': 'Stare + Gerundio',
        'Stare per + infinito': 'Stare per + Infinito',
        'Verbi modali (+ infinito)': 'Verbi Modali (+ infinito)',
        'Verbi modali + infinito': 'Verbi Modali (+ infinito)',
        'Verbi riflessivi (presente e passato prossimo)': 'Verbi Riflessivi (presente e passato prossimo)',
        'Volerci vs. Metterci': 'Volerci vs Metterci',
        'Sentire (Hear vs Smell vs Feel)': 'Sentire (hear vs smell vs feel)',
        
        # Vocabulary Topics (add/keep vocabolario)
        'Casa e quartiere': 'Casa e Quartiere (vocabolario)',
        'Casa e quartiere (vocabolario)': 'Casa e Quartiere (vocabolario)',
        'Descrizioni fisiche e del carattere': 'Descrizioni Fisiche e del Carattere (vocabolario)',
        'Famiglia (vocabolario)': 'Famiglia (vocabolario)',
        'Lavoro/ufficio (base)': 'Lavoro/Ufficio (vocabolario)',
        'Lavoro/ufficio (vocabolario)': 'Lavoro/Ufficio (vocabolario)',
        'Numeri oltre 100': 'Numeri Oltre 100 (vocabolario)',
        'Orario (vocabolario)': 'Orario (vocabolario)',
        'Ristorante (menu/prenotare/conti)': 'Ristorante (menu/prenotare/conti) (vocabolario)',
        'Routine quotidiana dettagliata': 'Routine Quotidiana Dettagliata (vocabolario)',
        'Salute (base)': 'Salute (vocabolario)',
        'Salute (vocabolario e frasi)': 'Salute (vocabolario)',
        'Scuola e università': 'Scuola e Università (vocabolario)',
        'Scuola/Università (base)': 'Scuola e Università (vocabolario)',
        'Shopping': 'Shopping (vocabolario)',
        'Shopping (vocabolario)': 'Shopping (vocabolario)',
        'Sport e hobby (vocabolario)': 'Sport e Hobby (vocabolario)',
        'Tempo (weather) – esteso': 'Tempo/Meteo (vocabolario)',
        'Tempo/Meteo (vocabolario)': 'Tempo/Meteo (vocabolario)',
        'Viaggi (trasporti, biglietti)': 'Viaggi (trasporti, biglietti) (vocabolario)',
        
        # Expression/Communication Topics (leave as is)
        'Programmi (piani futuri, inviti)': 'Programmi (piani futuri, inviti)',
        'Ricordi (narrazione semplice)': 'Ricordi (narrazione semplice)',
        
        # Verb Topics (special cases)
        'Presente indicativo: verbi regolari -are': 'Presente Indicativo: Verbi Regolari -are',
        'Presente indicativo: verbi regolari -ere': 'Presente Indicativo: Verbi Regolari -ere',
        'Presente indicativo: verbi regolari -ire': 'Presente Indicativo: Verbi Regolari -ire',
        'Verbo Essere e Presente Indicativo': 'Verbo Essere e Presente Indicativo',
        'Verbo essere': 'Verbo Essere e Presente Indicativo',
    }
    
    # Apply mappings
    for idx, row in df_consolidated.iterrows():
        old_topic = row['topic']
        new_topic = topic_mappings.get(old_topic, old_topic)
        
        if old_topic != new_topic:
            df_consolidated.at[idx, 'topic'] = new_topic
            changes[f"{old_topic} → {new_topic}"].append(idx)
    
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
        if topic not in topic_mappings.values():
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
    
    # Show merged topics with their new totals
    report.append("")
    report.append("MERGED TOPICS WITH NEW TOTALS")
    report.append("-" * 40)
    
    # Find topics that received merges (had questions added)
    merged_topics = {}
    for old_topic, new_topic in topic_mappings.items():
        if old_topic != new_topic:
            if new_topic not in merged_topics:
                merged_topics[new_topic] = []
            merged_topics[new_topic].append(old_topic)
    
    for new_topic in sorted(merged_topics.keys()):
        old_topics = merged_topics[new_topic]
        total_before = sum(topic_counts_before.get(t, 0) for t in old_topics + [new_topic])
        total_after = topic_counts_after.get(new_topic, 0)
        report.append(f"{new_topic}: {total_after} questions")
        report.append(f"  (merged from: {', '.join(old_topics)})")
    
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
        consolidated_df = consolidate_italian_topics_a2()
        
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
        print("1. The file 'Italian_A2.csv' exists in the current directory")
        print("2. The CSV file is properly formatted")
        print("3. You have write permissions in the current directory")
        
        import traceback
        print("\nFull error trace:")
        traceback.print_exc()