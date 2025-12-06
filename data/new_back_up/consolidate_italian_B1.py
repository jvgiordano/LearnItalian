import pandas as pd
import re
from collections import defaultdict
from datetime import datetime
import csv

def diagnose_csv_issues(input_file='Italian_B1.csv'):
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

def consolidate_italian_b1_topics(input_file='Italian_B1.csv', output_file='Italian_B1_consolidated.csv', report_file='consolidation_report_B1.txt'):
    """
    Consolidate Italian B1 grammar topics according to specified rules.
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
    report.append(f"Italian B1 Topic Consolidation Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 60)
    report.append("")
    
    # Track changes
    changes = defaultdict(list)
    topic_counts_before = df['topic'].value_counts().to_dict()
    
    # Define topic mappings for B1
    topic_mappings = {
        # Vocabulary topics (merging with "Vocabolario:" versions)
        'Ambiente': 'Ambiente (vocabolario e pratiche)',
        'Ambiente (pratiche)': 'Ambiente (vocabolario e pratiche)',
        'Vocabolario: Ambiente': 'Ambiente (vocabolario e pratiche)',
        'Business': 'Business e e-commerce (vocabolario)',
        'Business & e-commerce': 'Business e e-commerce (vocabolario)',
        'Vocabolario: Business': 'Business e e-commerce (vocabolario)',
        'Casa e quartiere': 'Casa e quartiere (problemi/soluzioni)',
        'Casa e quartiere (problemi/soluzioni)': 'Casa e quartiere (problemi/soluzioni)',
        'Cinema': 'Cinema (vocabolario)',
        'Cucina': 'Cucina (vocabolario)',
        'Cultura': 'Cultura (vocabolario)',
        'Educazione': 'Educazione (vocabolario)',
        'Gusti': 'Gusti (vocabolario)',
        'Hobby': 'Hobby (vocabolario)',
        'Lavoro': 'Lavoro (vocabolario e frasi)',
        'Vocabolario: Lavoro': 'Lavoro (vocabolario e frasi)',
        'Musica': 'Musica (vocabolario)',
        'Orario': 'Orario (vocabolario)',
        'Professioni': 'Professioni (vocabolario)',
        'Sentimenti': 'Sentimenti (vocabolario)',
        'Vocabolario: Sentimenti': 'Sentimenti (vocabolario)',
        'Shopping': 'Shopping (vocabolario)',
        'Società': 'Società (vocabolario)',
        'Vocabolario: Società': 'Società (vocabolario)',
        'Sport': 'Sport e hobby (vocabolario)',
        'Sport e hobby': 'Sport e hobby (vocabolario)',
        'Tecnologia': 'Tecnologia (uso quotidiano)',
        'Tecnologia (uso quotidiano)': 'Tecnologia (uso quotidiano)',
        'Vocabolario: Tecnologia': 'Tecnologia (uso quotidiano)',
        'Tempo': 'Tempo/Meteo (vocabolario)',
        'Trasporti': 'Trasporti (vocabolario)',
        'Viaggi': 'Viaggi (reclami/imprevisti)',
        'Viaggi (reclami/imprevisti)': 'Viaggi (reclami/imprevisti)',
        'Vocabolario: Viaggi': 'Viaggi (reclami/imprevisti)',
        
        # Expression/Communication topics
        'Burocrazia': 'Burocrazia',
        'Decisioni': 'Decisioni',
        'Descrizioni (dettaglio)': 'Descrizioni (dettaglio)',
        'Istruzione (esperienze scolastiche, esami)': 'Istruzione (esperienze scolastiche, esami)',
        'Media e attualità (notizie base, opinioni)': 'Media e attualità',
        'Memoria': 'Memoria e cambiamento',
        'Memoria e cambiamento': 'Memoria e cambiamento',
        'Relazioni': 'Relazioni',
        'Ricordi (narrazione estesa)': 'Ricordi (narrazione estesa)',
        'Ristorante (recensioni/preferenze)': 'Ristorante (recensioni/preferenze)',
        'Salute': 'Salute e medicina (sintomi, consigli)',
        'Salute (base)': 'Salute e medicina (sintomi, consigli)',
        'Salute e medicina (sintomi, consigli, visite mediche)': 'Salute e medicina (sintomi, consigli)',
        'Servizi': 'Servizi (banca, posta, reclami)',
        'Servizi (banca, posta, reclami)': 'Servizi (banca, posta, reclami)',
        
        # Grammar topics
        'Accordo del participio passato (con pronomi diretti, ne, riflessivi)': 'Accordo del participio passato',
        'Ci e ne (avanzato)': 'Ci e ne (avanzato)',
        'Comparativi e superlativi (irregolarità)': 'Comparativi e superlativi (irregolarità)',
        'Condizionale presente': 'Condizionale presente',
        'Congiuntivo': 'Congiuntivo presente',
        'Congiuntivo presente (introduzione: opinioni, emozioni base)': 'Congiuntivo presente',
        'Connettivi di causa/effetto/concessione/ordine)': 'Connettivi (causa/effetto/concessione)',
        'Discorso indiretto': 'Discorso indiretto',
        'Discorso indiretto (base)': 'Discorso indiretto',
        'Futuro semplice': 'Futuro semplice',
        'Futuro semplice (esteso)': 'Futuro semplice',
        'Gerundio': 'Gerundio (usi)',
        'Gerundio (usi)': 'Gerundio (usi)',
        'Imperativo': 'Imperativo (pronomi atoni, negazione)',
        'Imperativo (pronomi atoni; negazione)': 'Imperativo (pronomi atoni, negazione)',
        'Imperfetto vs Passato prossimo': 'Imperfetto vs Passato prossimo',
        'Particella ci': 'Particella ci',
        'Particella ne': 'Particella ne',
        'Participio passato': 'Participio passato',
        'Passivo': 'Passivo (con essere)',
        'Passivo con essere (tempi principali)': 'Passivo (con essere)',
        'Periodo ipotetico': 'Periodo ipotetico (I tipo)',
        'Periodo ipotetico I tipo': 'Periodo ipotetico (I tipo)',
        'Preposizioni': 'Preposizioni semplici',
        'Pronomi combinati': 'Pronomi combinati (glielo, me ne)',
        'Pronomi combinati (glielo, me ne, ecc.)': 'Pronomi combinati (glielo, me ne)',
        'Pronomi diretti': 'Pronomi diretti',
        'Pronomi indefiniti': 'Pronomi indefiniti',
        'Pronomi indefiniti (comuni)': 'Pronomi indefiniti',
        'Pronomi indiretti': 'Pronomi indiretti',
        'Pronomi relativi': 'Pronomi relativi (che/cui)',
        'Pronomi relativi (che/cui; prep + cui)': 'Pronomi relativi (che/cui)',
        'Si impersonale / si passivante (base)': 'Si impersonale/si passivante',
        'Stare + Gerundio': 'Stare + gerundio (progressivo)',
        'Stare + gerundio (progressivo)': 'Stare + gerundio (progressivo)',
        'Stare per + infinito': 'Stare per + infinito',
        'Trapassato prossimo': 'Trapassato prossimo',
        'Verbi Impersonali': 'Verbi impersonali',
        'Verbi causativi': 'Verbi causativi',
        'Verbi modali + pronomi clitici': 'Verbi modali + pronomi clitici',
    }
    
    # Apply mappings
    for idx, row in df_consolidated.iterrows():
        old_topic = row['topic']
        new_topic = topic_mappings.get(old_topic, old_topic)
        
        if old_topic != new_topic:
            df_consolidated.at[idx, 'topic'] = new_topic
            changes[f"{old_topic} → {new_topic}"].append(idx)
    
    # Check if there are any "Articoli" topics that need classification (unlikely in B1, but let's check)
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
    
    # Process any general Articoli questions if they exist
    articoli_mask = df_consolidated['topic'].isin(['Articoli', 'Articoli determinativi e indeterminativi'])
    if articoli_mask.sum() > 0:
        for idx in df_consolidated[articoli_mask].index:
            old_topic = df_consolidated.at[idx, 'topic']
            new_topic = classify_articolo(df_consolidated.loc[idx])
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
        if topic not in topic_mappings.values() and topic not in [
            'Articoli determinativi', 'Articoli indeterminativi', 'Articoli (generale)'
        ]:
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
        consolidated_df = consolidate_italian_b1_topics()
        
        # Print final topic distribution
        print("\n" + "=" * 60)
        print("FINAL TOPIC DISTRIBUTION (B1)")
        print("-" * 40)
        
        topic_counts = consolidated_df['topic'].value_counts()
        for topic, count in topic_counts.items():
            print(f"{topic}: {count} questions")
        
        print("-" * 40)
        print(f"TOTAL: {len(consolidated_df)} questions")
        
    except Exception as e:
        print(f"\nFatal error: {e}")
        print("\nPlease check that:")
        print("1. The file 'Italian_B1.csv' exists in the current directory")
        print("2. The CSV file is properly formatted")
        print("3. You have write permissions in the current directory")
        
        import traceback
        print("\nFull error trace:")
        traceback.print_exc()