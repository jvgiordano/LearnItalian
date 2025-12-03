#!/usr/bin/env python3
"""
Italian CEFR Question CSV Checker and Converter
Complete standalone script for validating, converting, and deduplicating Italian language questions.

Usage:
    python italian_csv_checker.py check input.csv
    python italian_csv_checker.py convert input.csv output.csv
    python italian_csv_checker.py dedupe input.csv output.csv --threshold 0.85
    python italian_csv_checker.py clean-db database.db --backup

Requirements:
    pip install pandas
"""

import csv
import re
import sqlite3
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


class ItalianCEFRQuestionConverter:
    """
    Converts and validates Italian CEFR question CSVs for the learning app.
    """
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.validation_errors = []
        self.warnings = []
        self.duplicates = []
        self.auto_corrections = []  # Track auto-corrections
        self.stats = {
            'total_processed': 0,
            'valid_questions': 0,
            'invalid_questions': 0,
            'duplicates_found': 0,
            'questions_enhanced': 0,
            'difficult_topics_found': 0,
            'topics_auto_corrected': 0  # New counter
        }
        
        # ALL VALID TOPICS - Now a single unified list regardless of level
        # This allows any topic to appear at any CEFR level
        self.ALL_VALID_TOPICS = [
            # A1 Topics
            "Presente indicativo: verbi regolari -are",
            "Presente indicativo: verbi regolari -ere", 
            "Presente indicativo: verbi regolari -ire",
            "Presente indicativo: verbi regolari -ire con -isc- (tipo finire)",
            "Presente Indicativo",
            "Verbo essere (to be)", "Verbo essere",
            "Verbo avere (to have)", "Verbo avere",
            "Verbi modali (uso base: potere, dovere, volere)", "Verbi modali",
            "Verbi Irregolari",
            "Verbi riflessivi (presente base)",
            "Piacere (uso base: piace/piacciono)", "Piacere",
            "Forme negative semplici", "Forme negative",
            "Parole interrogative",
            "C'è / Ci sono",
            "Preposizioni semplici (uso base)", "Preposizioni semplici", "Preposizioni",
            "Articoli determinativi e indeterminativi", "Articoli determinativi", "Articoli indeterminativi", "Articoli",
            "Articoli partitivi (base)",
            "Nomi: genere e numero", "Nomi",
            "Aggettivi: accordo base", "Aggettivi", "Aggettivi possessivi (base)", "Aggettivi possessivi",
            "Dimostrativi (base)", "Dimostrativi",
            "Pronomi soggetto", "Pronomi",
            "Giorni della settimana",
            "Mesi",
            "Stagioni",
            "Orario (telling time)", "Orario",
            "Espressioni di tempo basilari", "Espressioni di tempo",
            "Saluti, presentazioni, tu/Lei", "Saluti", "Saluti e presentazioni",
            "Direzioni e luoghi in città", "Direzioni", "Luoghi",
            "Numeri 0–100 e prezzi", "Numeri", "Numeri 0-100",
            "Colori",
            "Famiglia",
            "Cibo e bevande (base)", "Cibo e bevande", "Cibo",
            "Abbigliamento",
            "Casa (stanze e oggetti)", "Casa",
            "Corpo (base)", "Corpo",
            "Trasporti (base)", "Trasporti",
            "Professioni (base)", "Professioni",
            "Routine quotidiana",
            "Animali (comuni)", "Animali",
            "Ristorante (frasi base)", "Ristorante",
            "Geografia: paesi, città, nazionalità", "Geografia",
            "Meteo (base)", "Meteo",
            "Espressioni comuni",
            
            # A2 Topics
            "Passato prossimo (reg/irr; essere/avere; accordo)", "Passato prossimo",
            "Participio passato (comuni; irregolari frequenti)", "Participio passato",
            "Imperfetto (introduzione)", "Imperfetto",
            "Confronto articoli (def/indef/partitivi)",
            "Articoli partitivi",
            "Preposizioni articolate",
            "Verbi riflessivi (presente e passato prossimo)", "Verbi riflessivi",
            "Verbi modali + infinito",
            "Piacere (con pronomi; passato)",
            "Futuro semplice (base)", "Futuro semplice",
            "Condizionale presente",
            "Stare per + infinito",
            "Imperativo (tu/noi/voi; base)", "Imperativo",
            "Pronomi diretti",
            "Pronomi indiretti",
            "Particella ne (base)", "Particella ne",
            "Particella ci (base)", "Particella ci",
            "Avverbi di frequenza/tempo/luogo", "Avverbi",
            "Comparativi e superlativi (base)", "Comparativi", "Superlativi",
            "Espressioni di tempo (fa, da, tra/fra; ore)",
            "Numeri oltre 100",
            "Shopping",
            "Viaggi (trasporti, biglietti)", "Viaggi",
            "Casa e quartiere",
            "Lavoro/ufficio (base)", "Lavoro",
            "Scuola/Università (base)", "Scuola", "Università",
            "Salute (base)", "Salute",
            "Ristorante (menu/prenotare/conti)",
            "Routine quotidiana dettagliata",
            "Tempo (weather) – esteso", "Tempo",
            "Ricordi (narrazione semplice)", "Ricordi",
            "Programmi (piani futuri, inviti)", "Programmi",
            "Descrizioni fisiche e del carattere", "Descrizioni",
            "Sport e Hobby",
            "Aggettivi Dimostrativi",
            "Stare + Gerundio",
            
            # B1 Topics
            "Memoria",
            "Musica",
            "Gusti",
            "Saluti",
            "Cucina",
            "Cinema",
            "Imperfetto vs Passato prossimo",
            "Trapassato prossimo",
            "Futuro semplice (esteso)",
            "Congiuntivo presente (introduzione: opinioni, emozioni base)", "Congiuntivo presente",
            "Accordo del participio passato (con pronomi diretti, ne, riflessivi)", "Accordo del participio passato",
            "Periodo ipotetico I tipo", "Periodo ipotetico",
            "Stare + gerundio (progressivo)",
            "Gerundio (usi)", "Gerundio",
            "Imperativo (pronomi atoni; negazione)",
            "Pronomi combinati (glielo, me ne, ecc.)", "Pronomi combinati",
            "Ci e ne (avanzato)", "Ci e ne",
            "Pronomi relativi (che/cui; prep + cui)", "Pronomi relativi",
            "Pronomi indefiniti (comuni)", "Pronomi indefiniti",
            "Passivo con essere (tempi principali)", "Passivo",
            "Si impersonale / si passivante (base)", "Si impersonale", "Si passivante",
            "Discorso indiretto (base)", "Discorso indiretto",
            "Connettivi di causa/effetto/concessione/ordine)", "Connettivi",
            "Comparativi e superlativi (irregolarità)", "Comparativi e superlativi",
            "Verbi modali + pronomi clitici",
            "Viaggi (reclami/imprevisti)",
            "Sport e hobby", "Sport", "Hobby",
            "Tecnologia (uso quotidiano)", "Tecnologia",
            "Casa e quartiere (problemi/soluzioni)",
            "Relazioni",
            "Ambiente (pratiche)", "Ambiente",
            "Ristorante (recensioni/preferenze)",
            "Salute e medicina (sintomi, consigli, visite mediche)", "Salute e medicina",
            "Media e attualità (notizie base, opinioni)", "Media e attualità",
            "Servizi (banca, posta, reclami)", "Servizi",
            "Istruzione (esperienze scolastiche, esami)", "Istruzione",
            "Ricordi (narrazione estesa)",
            "Descrizioni (dettaglio)",
            
            # B2 Topics
            "Congiuntivo passato",
            "Congiuntivo imperfetto",
            "Concordanza dei tempi (casi tipici)", "Concordanza dei tempi",
            "Periodo ipotetico II e III",
            "Condizionale passato",
            "Futuro anteriore",
            "Forme implicite (infinito, gerundio, participio con valore temporale/causale)", "Forme implicite",
            "Passivo con essere/venire; si passivante (avanzato)",
            "Verbi causativi (fare + infinito)", "Verbi causativi",
            "Pronomi relativi avanzati (il quale; cui articolate)",
            "Pronomi indefiniti/dimostrativi (avanzato)", "Pronomi dimostrativi",
            "Costruzioni con gerundio/participio", "Costruzioni",
            "Preposizioni complesse e locuzioni", "Preposizioni complesse",
            "Registro e toni (formale/informale)", "Registro",
            "Connettivi complessi (benché, sebbene, purché, ecc.)", "Connettivi complessi",
            "Verbi pronominali (andarsene, cavarsela, farcela, ecc.)", "Verbi pronominali",
            "Discorso indiretto (avanzato)",
            "Carattere e personalità (avanzato)", "Carattere e personalità",
            "Cultura",
            "Politica e diritto (lessico generale)", "Politica e diritto",
            "Economia e finanza (lessico generale)", "Economia e finanza",
            "Sanità e società", "Sanità",
            "Ambiente (dibattito)",
            "Tecnologia (privacy/AI/social)",
            "Business & e-commerce", "Business",
            "Trasporti (norme/sostenibilità)",
            "Professioni (carriere)",
            "Burocrazia (pratiche)", "Burocrazia",
            "Lessico Legacy",
            "Linguagio Formale",
            "Linguagio Academico",
            "Lessico",
            
            
            # C1 Topics
            "Congiuntivo vs indicativo (scelte stilistiche)", "Congiuntivo",
            "Concordanza dei tempi (casi complessi)",
            "Discorso indiretto avanzato (deissi/tempi)",
            "Participio passato assoluto; costruzioni assolute", "Participio passato assoluto",
            "Stile inverso e focalizzazioni",
            "Passato remoto (uso letterario/storico)", "Passato remoto",
            "Trapassato remoto (ricettivo)", "Trapassato remoto",
            "Andare + participio (valore di dovere)",
            "Si impersonale/passivante (sfumature/ambiguità)",
            "Nominalizzazioni e densità informativa", "Nominalizzazioni",
            "Collocazioni e fraseologia", "Collocazioni",
            "Lessico formale/accademico", "Lessico formale",
            "Espressioni idiomatiche e proverbi", "Espressioni idiomatiche",
            "Connettivi formali e marcatori discorsivi", "Connettivi formali",
            "Lessico legale",
            "Lessico business",
            "Lessico medico/sanitario", "Lessico medico",
            "Lessico tecnologico",
            "Lessico agricolo",
            "Lessico enologico",
            "Lessico figurato/metaforico", "Lessico figurato",
            "Filosofia e pensiero critico", "Filosofia",
            "Ricerca accademica",
            "Negoziazione e diplomazia", "Negoziazione",
            "Cultura (analisi/commento)",
            "Società (valori/demografia)", "Società",
            "Politica e diritto (argomentazione)",
            "Economia e finanza (analisi)",
            "Ambiente (policy/dibattito)",
            "Urbanistica e trasporti", "Urbanistica",
            "Burocrazia (procedure avanzate)",
            "Professioni (settoriale)",
            "Memoria e cambiamento (saggi)", "Memoria e cambiamento",
            "Sentimenti e stati d'animo (lessico fine)", "Sentimenti",
            "Decisioni (pro/contro)", "Decisioni",
            "Musica e cinema (recensioni formali)", "Musica e cinema",
            "Educazione (accademia, università)", "Educazione",
            "Business & e-commerce (strategie)",
        ]
        
        # Remove duplicates from the unified list while preserving order
        seen = set()
        self.ALL_VALID_TOPICS = [x for x in self.ALL_VALID_TOPICS if not (x in seen or seen.add(x))]
        
        # Resource URL mapping
        self.TOPIC_RESOURCES = {
            "Passato prossimo": "https://www.lawlessitalian.com/grammar/passato-prossimo/",
            "Imperfetto": "https://www.lawlessitalian.com/grammar/imperfetto/",
            "Presente indicativo": "https://www.lawlessitalian.com/grammar/present-tense/",
            "Futuro semplice": "https://www.lawlessitalian.com/grammar/future-tense/",
            "Condizionale": "https://www.lawlessitalian.com/grammar/conditional/",
            "Congiuntivo": "https://www.lawlessitalian.com/grammar/subjunctive/",
            "Articoli": "https://www.lawlessitalian.com/grammar/articles/",
            "Articoli partitivi": "https://www.lawlessitalian.com/grammar/partitive-articles/",
            "Preposizioni": "https://www.lawlessitalian.com/grammar/prepositions/",
            "Verbo essere": "https://www.lawlessitalian.com/grammar/essere-to-be/",
            "Verbo avere": "https://www.lawlessitalian.com/grammar/avere-to-have/",
            "Verbi modali": "https://www.lawlessitalian.com/grammar/modal-verbs/",
            "Pronomi": "https://www.lawlessitalian.com/grammar/pronouns/",
            "Imperativo": "https://www.lawlessitalian.com/grammar/imperative/",
            "Gerundio": "https://www.lawlessitalian.com/grammar/gerund/",
            "Numeri": "https://www.lawlessitalian.com/vocabulary/numbers/",
            "Giorni": "https://www.lawlessitalian.com/vocabulary/days-of-the-week/",
            "Mesi": "https://www.lawlessitalian.com/vocabulary/months/",
            "Famiglia": "https://www.lawlessitalian.com/vocabulary/family/",
            "Colori": "https://www.lawlessitalian.com/vocabulary/colors/",
            "Cibo": "https://www.lawlessitalian.com/vocabulary/food/",
            "Casa": "https://www.lawlessitalian.com/vocabulary/house/",
            "Corpo": "https://www.lawlessitalian.com/vocabulary/body/",
            "Saluti": "https://www.lawlessitalian.com/vocabulary/greetings/",
            "Espressioni": "https://www.lawlessitalian.com/vocabulary/expressions/",
        }
        
        # Difficult topics
        self.DIFFICULT_TOPICS = {
            "Passato prossimo (reg/irr; essere/avere; accordo)",
            "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
            "Imperfetto vs Passato prossimo",
            "Congiuntivo presente (introduzione: opinioni, emozioni base)",
            "Congiuntivo presente",
            "Congiuntivo passato",
            "Congiuntivo imperfetto",
            "Concordanza dei tempi",
            "Periodo ipotetico II e III",
            "Pronomi combinati (glielo, me ne, ecc.)",
            "Si impersonale / si passivante",
        }
        
        # Legacy topic mappings (expanded)
        self.LEGACY_TOPIC_MAPPINGS = {
            # A1 mappings
            "Articoli Partitivi": "Articoli partitivi (base)",
            "Articoli partitivi": "Articoli partitivi (base)",
            "Partitivi": "Articoli partitivi (base)",
            "Verbi Riflessivi": "Verbi riflessivi (presente base)",
            "Verbi riflessivi": "Verbi riflessivi (presente base)",
            "Espressioni": {
                "default": "Espressioni comuni",
                "keywords": {
                    "tempo|ora|quando|fa|dopo|prima": "Espressioni di tempo basilari",
                    "ciao|buon|salve|piacere|come sta": "Saluti, presentazioni, tu/Lei",
                }
            },
            "Presente": "Presente indicativo: verbi regolari -are",
            "Presente indicativo": "Presente indicativo: verbi regolari -are",
            "Verbi regolari": "Presente indicativo: verbi regolari -are",
            "Verbi -are": "Presente indicativo: verbi regolari -are",
            "Verbi -ere": "Presente indicativo: verbi regolari -ere",
            "Verbi -ire": "Presente indicativo: verbi regolari -ire",
            "Articoli": "Articoli determinativi e indeterminativi",
            "Articoli determinativi": "Articoli determinativi e indeterminativi",
            "Articoli indeterminativi": "Articoli determinativi e indeterminativi",
            "Numeri": "Numeri 0–100 e prezzi",
            "Numeri e prezzi": "Numeri 0–100 e prezzi",
            "Tempo": {
                "default": "Espressioni di tempo basilari",
                "keywords": {
                    "piove|sole|neve|freddo|caldo|tempo atmosferico": "Meteo (base)",
                }
            },
            "Verbo essere": "Verbo essere (to be)",
            "Essere": "Verbo essere (to be)",
            "Verbo avere": "Verbo avere (to have)",
            "Avere": "Verbo avere (to have)",
            "Verbi irregolari": "Verbi Irregolari",
            "Preposizioni": "Preposizioni semplici (uso base)",
            "Preposizioni semplici": "Preposizioni semplici (uso base)",
            "Aggettivi": "Aggettivi: accordo base",
            "Aggettivi accordo": "Aggettivi: accordo base",
            "Aggettivi possessivi": "Aggettivi possessivi (base)",
            "Possessivi": "Aggettivi possessivi (base)",
            "Pronomi": "Pronomi soggetto",
            "Pronomi personali": "Pronomi soggetto",
            "Direzioni": "Direzioni e luoghi in città",
            "Luoghi": "Direzioni e luoghi in città",
            "Città": "Direzioni e luoghi in città",
            "Saluti": "Saluti, presentazioni, tu/Lei",
            "Presentazioni": "Saluti, presentazioni, tu/Lei",
            "Tu/Lei": "Saluti, presentazioni, tu/Lei",
            "Giorni": "Giorni della settimana",
            "Settimana": "Giorni della settimana",
            "Orario": "Orario (telling time)",
            "Ore": "Orario (telling time)",
            "Cibo": "Cibo e bevande (base)",
            "Bevande": "Cibo e bevande (base)",
            "Casa": "Casa (stanze e oggetti)",
            "Stanze": "Casa (stanze e oggetti)",
            "Corpo": "Corpo (base)",
            "Trasporti": "Trasporti (base)",
            "Professioni": "Professioni (base)",
            "Lavoro": "Professioni (base)",
            "Routine": "Routine quotidiana",
            "Quotidiano": "Routine quotidiana",
            "Animali": "Animali (comuni)",
            "Ristorante": "Ristorante (frasi base)",
            "Geografia": "Geografia: paesi, città, nazionalità",
            "Nazionalità": "Geografia: paesi, città, nazionalità",
            "Paesi": "Geografia: paesi, città, nazionalità",
            "Meteo": "Meteo (base)",
            "Tempo atmosferico": "Meteo (base)",
            
            # A2 mappings
            "Passato": "Passato prossimo (reg/irr; essere/avere; accordo)",
            "Passato prossimo": "Passato prossimo (reg/irr; essere/avere; accordo)",
            "Participio": "Participio passato (comuni; irregolari frequenti)",
            "Participio passato": "Participio passato (comuni; irregolari frequenti)",
            "Imperfetto": "Imperfetto (introduzione)",
            "Futuro": "Futuro semplice (base)",
            "Futuro semplice": "Futuro semplice (base)",
            "Imperativo": "Imperativo (tu/noi/voi; base)",
            "Pronomi diretti": "Pronomi diretti",
            "Oggetto diretto": "Pronomi diretti",
            "Pronomi indiretti": "Pronomi indiretti",
            "Oggetto indiretto": "Pronomi indiretti",
            "Ne": "Particella ne (base)",
            "Ci": "Particella ci (base)",
            "Avverbi": "Avverbi di frequenza/tempo/luogo",
            "Comparativi": "Comparativi e superlativi (base)",
            "Superlativi": "Comparativi e superlativi (base)",
            "Tempo (espressioni)": "Espressioni di tempo (fa, da, tra/fra; ore)",
            "Espressioni di tempo": "Espressioni di tempo (fa, da, tra/fra; ore)",
            "Interrogativi": "Parole interrogative",
            "Shopping": "Shopping",
            "Viaggi": "Viaggi (trasporti, biglietti)",
            "Viaggio": "Viaggi (trasporti, biglietti)",
            "Casa e quartiere": "Casa e quartiere",
            "Quartiere": "Casa e quartiere",
            "Lavoro": "Lavoro/ufficio (base)",
            "Ufficio": "Lavoro/ufficio (base)",
            "Scuola": "Scuola/Università (base)",
            "Università": "Scuola/Università (base)",
            "Salute": "Salute (base)",
            "Medicina": "Salute (base)",
            "Ristorante avanzato": "Ristorante (menu/prenotare/conti)",
            "Menu": "Ristorante (menu/prenotare/conti)",
            "Routine dettagliata": "Routine quotidiana dettagliata",
            "Ricordi": "Ricordi (narrazione semplice)",
            "Narrazione": "Ricordi (narrazione semplice)",
            "Programmi": "Programmi (piani futuri, inviti)",
            "Piani": "Programmi (piani futuri, inviti)",
            "Inviti": "Programmi (piani futuri, inviti)",
            "Descrizioni": "Descrizioni fisiche e del carattere",
            "Carattere": "Descrizioni fisiche e del carattere",
            
            # B1 mappings
            "Imperfetto vs Passato": "Imperfetto vs Passato prossimo",
            "Trapassato": "Trapassato prossimo",
            "Trapassato prossimo": "Trapassato prossimo",
            "Condizionale": "Condizionale presente",
            "Condizionale presente": "Condizionale presente",
            "Congiuntivo": "Congiuntivo presente (introduzione: opinioni, emozioni base)",
            "Congiuntivo presente": "Congiuntivo presente (introduzione: opinioni, emozioni base)",
            "Accordo participio": "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
            "Periodo ipotetico": "Periodo ipotetico I tipo",
            "Periodo ipotetico I": "Periodo ipotetico I tipo",
            "Stare + gerundio": "Stare + gerundio (progressivo)",
            "Progressivo": "Stare + gerundio (progressivo)",
            "Gerundio": "Gerundio (usi)",
            "Pronomi combinati": "Pronomi combinati (glielo, me ne, ecc.)",
            "Pronomi doppi": "Pronomi combinati (glielo, me ne, ecc.)",
            "Pronomi relativi": "Pronomi relativi (che/cui; prep + cui)",
            "Che/cui": "Pronomi relativi (che/cui; prep + cui)",
            "Pronomi indefiniti": "Pronomi indefiniti (comuni)",
            "Indefiniti": "Pronomi indefiniti (comuni)",
            "Passivo": "Passivo con essere (tempi principali)",
            "Voce passiva": "Passivo con essere (tempi principali)",
            "Si impersonale": "Si impersonale / si passivante (base)",
            "Si passivante": "Si impersonale / si passivante (base)",
            "Discorso indiretto": "Discorso indiretto (base)",
            "Reported speech": "Discorso indiretto (base)",
            "Connettivi": "Connettivi di causa/effetto/concessione/ordine)",
            "Congiunzioni": "Connettivi di causa/effetto/concessione/ordine)",
            "Sport": "Sport e hobby",
            "Hobby": "Sport e hobby",
            "Tecnologia": "Tecnologia (uso quotidiano)",
            "Computer": "Tecnologia (uso quotidiano)",
            "Internet": "Tecnologia (uso quotidiano)",
            "Relazioni": "Relazioni",
            "Amicizia": "Relazioni",
            "Amore": "Relazioni",
            "Ambiente": "Ambiente (pratiche)",
            "Ecologia": "Ambiente (pratiche)",
            "Salute e medicina": "Salute e medicina (sintomi, consigli, visite mediche)",
            "Sintomi": "Salute e medicina (sintomi, consigli, visite mediche)",
            "Media": "Media e attualità (notizie base, opinioni)",
            "Notizie": "Media e attualità (notizie base, opinioni)",
            "Attualità": "Media e attualità (notizie base, opinioni)",
            "Servizi": "Servizi (banca, posta, reclami)",
            "Banca": "Servizi (banca, posta, reclami)",
            "Posta": "Servizi (banca, posta, reclami)",
            "Istruzione": "Istruzione (esperienze scolastiche, esami)",
            "Esami": "Istruzione (esperienze scolastiche, esami)",
            
            # B2 and C1 mappings
            "Congiuntivo passato": "Congiuntivo passato",
            "Congiuntivo imperfetto": "Congiuntivo imperfetto",
            "Concordanza": "Concordanza dei tempi (casi tipici)",
            "Concordanza dei tempi": "Concordanza dei tempi (casi tipici)",
            "Periodo ipotetico II": "Periodo ipotetico II e III",
            "Periodo ipotetico III": "Periodo ipotetico II e III",
            "Condizionale passato": "Condizionale passato",
            "Futuro anteriore": "Futuro anteriore",
            "Forme implicite": "Forme implicite (infinito, gerundio, participio con valore temporale/causale)",
            "Verbi causativi": "Verbi causativi (fare + infinito)",
            "Fare + infinito": "Verbi causativi (fare + infinito)",
            "Verbi pronominali": "Verbi pronominali (andarsene, cavarsela, farcela, ecc.)",
            "Cultura": "Cultura",
            "Politica": "Politica e diritto (lessico generale)",
            "Diritto": "Politica e diritto (lessico generale)",
            "Economia": "Economia e finanza (lessico generale)",
            "Finanza": "Economia e finanza (lessico generale)",
            "Business": "Business & e-commerce",
            "E-commerce": "Business & e-commerce",
            "Lessico formale": "Lessico formale/accademico",
            "Lessico accademico": "Lessico formale/accademico",
            "Idiomi": "Espressioni idiomatiche e proverbi",
            "Proverbi": "Espressioni idiomatiche e proverbi",
        }
        
        # Track topic mapping usage
        self.topic_mapping_usage = {}
        # Track unmapped topics
        self.unmapped_topics = {}
    
    def convert_csv(self, input_file: str, output_file: str = None,
                   check_duplicates: bool = True,
                   enhance_questions: bool = True,
                   validate_only: bool = False) -> Dict:
        """Main conversion function."""
        
        # Read input CSV with better error handling
        df = None
        
        # Try different parsing strategies
        strategies = [
            {'encoding': 'utf-8', 'on_bad_lines': 'skip'},
            {'encoding': 'utf-8', 'quoting': csv.QUOTE_ALL},
            {'encoding': 'latin-1', 'on_bad_lines': 'skip'},
            {'encoding': 'latin-1', 'quoting': csv.QUOTE_ALL},
        ]
        
        for strategy in strategies:
            try:
                df = pd.read_csv(input_file, **strategy)
                if 'on_bad_lines' in strategy:
                    # Check if lines were skipped
                    with open(input_file, 'r', encoding=strategy['encoding']) as f:
                        total_lines = sum(1 for line in f) - 1  # Subtract header
                    if len(df) < total_lines:
                        print(f"⚠️  Warning: Skipped {total_lines - len(df)} malformed lines")
                break
            except Exception as e:
                continue
        
        # If all strategies fail, try to diagnose the problem
        if df is None:
            print(f"❌ Failed to parse CSV. Attempting to diagnose...")
            self.diagnose_csv_issues(input_file)
            return self.generate_report()
        
        converted_questions = []
        
        print(f"Processing {len(df)} questions...")
        
        for idx, row in df.iterrows():
            self.stats['total_processed'] += 1
            
            if self.verbose and idx % 100 == 0:
                print(f"  Processed {idx}/{len(df)} questions...")
            
            # Convert and validate each question
            converted_q = self.convert_question(row, idx)
            
            if converted_q:
                # Check for duplicates
                if check_duplicates and self.is_duplicate(converted_q, converted_questions):
                    self.duplicates.append((idx, converted_q['question_text']))
                    self.stats['duplicates_found'] += 1
                    continue
                
                # Enhance question
                if enhance_questions:
                    converted_q = self.enhance_question(converted_q)
                    self.stats['questions_enhanced'] += 1
                
                # Check if difficult topic
                if converted_q['topic'] in self.DIFFICULT_TOPICS:
                    self.stats['difficult_topics_found'] += 1
                
                # Validate
                if self.validate_question(converted_q, idx):
                    converted_questions.append(converted_q)
                    self.stats['valid_questions'] += 1
                else:
                    self.stats['invalid_questions'] += 1
        
        # Write output
        if not validate_only and output_file and converted_questions:
            self.write_output_csv(converted_questions, output_file)
            print(f"✅ Wrote {len(converted_questions)} questions to {output_file}")
        
        return self.generate_report()
    
    def diagnose_csv_issues(self, input_file: str):
        """Diagnose common CSV parsing issues."""
        print("\nDiagnosing CSV issues...")
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(input_file, 'r', encoding='latin-1') as f:
                lines = f.readlines()
        
        if not lines:
            print("❌ File is empty")
            return
        
        # Check header
        header = lines[0].strip()
        header_fields = len(header.split(','))
        print(f"Header has {header_fields} fields")
        
        # Check problematic lines
        problems = []
        for i, line in enumerate(lines[1:], start=2):  # Start from line 2 (after header)
            if not line.strip():
                continue
            
            # Count fields (simple split)
            field_count = len(line.split(','))
            
            # Count quoted fields
            quoted_count = line.count('"')
            
            if field_count != header_fields:
                problems.append({
                    'line': i,
                    'expected': header_fields,
                    'found': field_count,
                    'quotes': quoted_count,
                    'preview': line[:100].strip()
                })
        
        if problems:
            print(f"\n❌ Found {len(problems)} problematic lines:")
            for p in problems[:5]:  # Show first 5
                print(f"\n  Line {p['line']}:")
                print(f"    Expected fields: {p['expected']}")
                print(f"    Found fields: {p['found']}")
                print(f"    Quote marks: {p['quotes']}")
                print(f"    Preview: {p['preview']}...")
                
                # Suggest issue
                if p['quotes'] % 2 != 0:
                    print(f"    💡 Odd number of quotes - likely unescaped quote in text")
                elif p['found'] > p['expected']:
                    print(f"    💡 Too many fields - likely unescaped comma in text")
                elif p['found'] < p['expected']:
                    print(f"    💡 Too few fields - likely missing data")
            
            if len(problems) > 5:
                print(f"\n  ... and {len(problems) - 5} more problematic lines")
            
            print("\n💡 SUGGESTED FIXES:")
            print("  1. Open the CSV in Excel/LibreOffice and re-save as CSV")
            print("  2. Use a text editor to fix lines manually")
            print("  3. Ensure all text fields with commas/quotes are properly quoted")
            print("  4. Try using --fix flag to attempt automatic repair (coming soon)")
        else:
            print("✅ No obvious structural issues found")
            print("The issue might be with encoding or special characters")
    
    def convert_question(self, row: pd.Series, idx: int) -> Optional[Dict]:
        """Converts a single question."""
        try:
            # Handle potential field name variations
            question_dict = {}
            
            # Map common field variations
            field_mappings = {
                'question_text': ['question_text', 'question', 'Question'],
                'english_translation': ['english_translation', 'translation', 'English'],
                'option_a': ['option_a', 'optionA', 'a', 'A'],
                'option_b': ['option_b', 'optionB', 'b', 'B'],
                'option_c': ['option_c', 'optionC', 'c', 'C'],
                'option_d': ['option_d', 'optionD', 'd', 'D'],
                'correct_option': ['correct_option', 'correct', 'answer', 'Answer'],
                'cefr_level': ['cefr_level', 'level', 'Level', 'CEFR'],
                'topic': ['topic', 'Topic', 'category'],
                'explanation': ['explanation', 'Explanation', 'note'],
                'hint': ['hint', 'Hint', 'clue'],
                'resource': ['resource', 'Resource', 'url', 'link'],
                'alternate_correct_responses': ['alternate_correct_responses', 'alternates', 'alt'],
                'complete_sentence': ['complete_sentence', 'complete', 'full']
            }
            
            for standard_field, variations in field_mappings.items():
                value = ''
                for variant in variations:
                    if variant in row.index:
                        value = str(row[variant]) if pd.notna(row[variant]) else ''
                        break
                question_dict[standard_field] = value.strip()
            
            # Ensure correct_option is uppercase
            question_dict['correct_option'] = question_dict['correct_option'].upper()
            
            # Generate complete sentence
            question_dict['complete_sentence'] = self.generate_complete_sentence(question_dict)
            
            return question_dict
            
        except Exception as e:
            self.validation_errors.append(f"Row {idx}: Error converting - {str(e)}")
            return None
    
    def generate_complete_sentence(self, question: Dict) -> str:
        """Generates the complete sentence."""
        question_text = question['question_text']
        
        if question_text.startswith('Write:'):
            correct_option = question['correct_option']
            if correct_option in ['A', 'B', 'C', 'D']:
                return question[f'option_{correct_option.lower()}']
            return question_text
        
        if '___' not in question_text:
            return question_text
        
        correct_option = question['correct_option']
        if correct_option in ['A', 'B', 'C', 'D']:
            correct_answer = question[f'option_{correct_option.lower()}']
            complete = question_text.replace('___', correct_answer, 1)
            complete = re.sub(r'\s*\([^)]*\)\s*$', '', complete)
            return complete.strip()
        
        return question_text
    
    def enhance_question(self, question: Dict) -> Dict:
        """Enhances questions with additional information."""
        # Auto-correct topic capitalization and close matches
        question = self.auto_correct_topic(question)
        
        # Extract verb hints
        if not question['hint'] and '___' in question['question_text']:
            verb_match = re.search(r'\(([^)]+)\)', question['question_text'])
            if verb_match:
                verb_hint = verb_match.group(1)
                if verb_hint.endswith(('are', 'ere', 'ire')) or verb_hint in ['formal', 'informal', 'feminine', 'masculine']:
                    question['hint'] = verb_hint
        
        # Add resource URLs
        if not question['resource'] or 'lawlessitalian.com/grammar/' in question['resource']:
            question['resource'] = self.get_resource_url(question['topic'])
        
        # Generate alternate responses
        if not question['alternate_correct_responses']:
            question['alternate_correct_responses'] = self.generate_alternates(question)
        
        return question
    
    def auto_correct_topic(self, question: Dict) -> Dict:
        """Auto-correct topic capitalization, close matches, and legacy topic names.
        Now works across all levels - any topic can appear at any level."""
        
        current_topic = question['topic']
        original_topic = current_topic  # Keep track of original
        
        # First, try to map legacy topics - no level restriction
        mapped_topic = self.map_legacy_topic(current_topic, question.get('question_text', ''))
        if mapped_topic != current_topic:
            self.auto_corrections.append(f"'{current_topic}' → '{mapped_topic}' (legacy mapping)")
            if self.verbose:
                print(f"  Mapped legacy topic: '{current_topic}' → '{mapped_topic}'")
            question['topic'] = mapped_topic
            self.stats['topics_auto_corrected'] += 1
            return question
        
        # Check against ALL valid topics, regardless of level
        valid_topics = self.ALL_VALID_TOPICS
        
        # Check for exact match (case-sensitive)
        if current_topic in valid_topics:
            return question
        
        # Check for case-insensitive exact match
        for valid_topic in valid_topics:
            if current_topic.lower() == valid_topic.lower():
                self.auto_corrections.append(f"'{original_topic}' → '{valid_topic}' (capitalization)")
                if self.verbose:
                    print(f"  Auto-corrected: '{original_topic}' → '{valid_topic}'")
                question['topic'] = valid_topic
                self.stats['topics_auto_corrected'] += 1
                return question
        
        # Check for very close matches (>90% similarity)
        best_match = None
        best_score = 0
        
        for valid_topic in valid_topics:
            score = self.similarity_ratio(current_topic, valid_topic)
            if score > best_score:
                best_score = score
                best_match = valid_topic
        
        if best_score > 0.9:  # 90% similarity threshold for auto-correction
            self.auto_corrections.append(f"'{original_topic}' → '{best_match}' ({best_score:.1%} match)")
            if self.verbose:
                print(f"  Auto-corrected: '{original_topic}' → '{best_match}' (similarity: {best_score:.1%})")
            question['topic'] = best_match
            self.stats['topics_auto_corrected'] += 1
        elif best_score > 0.85:  # Between 85-90%, just warn but don't auto-correct
            level = question.get('cefr_level', 'Unknown')
            self.warnings.append(f"Topic '{original_topic}' is similar to '{best_match}' ({best_score:.1%}) - consider manual correction")
            # Track unmapped topic
            key = f"{level}: {original_topic}"
            if key not in self.unmapped_topics:
                self.unmapped_topics[key] = {'count': 0, 'best_match': best_match, 'score': best_score}
            self.unmapped_topics[key]['count'] += 1
        else:
            # No good match found, warn about unknown topic
            level = question.get('cefr_level', 'Unknown')
            self.warnings.append(f"Unknown topic '{original_topic}' for {level} - no suitable mapping found")
            # Track unmapped topic
            key = f"{level}: {original_topic}"
            if key not in self.unmapped_topics:
                self.unmapped_topics[key] = {'count': 0, 'best_match': best_match, 'score': best_score}
            self.unmapped_topics[key]['count'] += 1
        
        return question
    
    def map_legacy_topic(self, topic: str, question_text: str = "") -> str:
        """Map legacy topic names to new standardized names.
        Now works without level restrictions - looks for mappings across all topics."""
        
        # Check if it's already a valid topic
        if topic in self.ALL_VALID_TOPICS:
            return topic
        
        # Track original for reporting
        original_topic = topic
        mapped_topic = None
        
        # Check direct mappings
        if topic in self.LEGACY_TOPIC_MAPPINGS:
            mapping = self.LEGACY_TOPIC_MAPPINGS[topic]
            
            # Handle conditional mappings based on question content
            if isinstance(mapping, dict):
                # Check keywords in question text
                if "keywords" in mapping and question_text:
                    for keyword_pattern, target_topic in mapping["keywords"].items():
                        if re.search(keyword_pattern, question_text.lower()):
                            mapped_topic = target_topic
                            break
                
                # Use default if no keyword match
                if not mapped_topic:
                    mapped_topic = mapping.get("default", topic)
            else:
                # Direct string mapping
                mapped_topic = mapping
            
            # Track the mapping usage
            if mapped_topic != topic:
                key = f"{original_topic} → {mapped_topic}"
                self.topic_mapping_usage[key] = self.topic_mapping_usage.get(key, 0) + 1
                return mapped_topic
        
        # Try to find best match in ALL valid topics
        best_match = None
        best_score = 0
        
        for valid_topic in self.ALL_VALID_TOPICS:
            # Check if the legacy topic is contained in the new topic
            if topic.lower() in valid_topic.lower():
                score = len(topic) / len(valid_topic)  # Preference for more specific matches
                if score > best_score:
                    best_score = score
                    best_match = valid_topic
            
            # Check if the new topic contains the legacy topic
            elif valid_topic.lower() in topic.lower():
                score = len(valid_topic) / len(topic) * 0.9  # Slightly lower score
                if score > best_score:
                    best_score = score
                    best_match = valid_topic
        
        if best_match and best_score > 0.5:
            key = f"{original_topic} → {best_match}"
            self.topic_mapping_usage[key] = self.topic_mapping_usage.get(key, 0) + 1
            return best_match
        
        # No mapping found, return original
        return topic
    
    def generate_mapping_report(self) -> str:
        """Generate a report of all topic mappings used."""
        if not self.topic_mapping_usage:
            return "No legacy topic mappings were needed."
        
        report = ["TOPIC MAPPING REPORT", "=" * 50]
        
        # Sort by frequency
        sorted_mappings = sorted(self.topic_mapping_usage.items(), key=lambda x: x[1], reverse=True)
        
        for mapping, count in sorted_mappings:
            report.append(f"{count:3d}x: {mapping}")
        
        report.append("=" * 50)
        report.append(f"Total mappings: {sum(self.topic_mapping_usage.values())}")
        
        return "\n".join(report)
    
    def get_resource_url(self, topic: str) -> str:
        """Get resource URL for topic."""
        if topic in self.TOPIC_RESOURCES:
            return self.TOPIC_RESOURCES[topic]
        
        topic_lower = topic.lower()
        for key, url in self.TOPIC_RESOURCES.items():
            if key.lower() in topic_lower or topic_lower in key.lower():
                return url
        
        return "https://www.lawlessitalian.com/grammar/"
    
    def generate_alternates(self, question: Dict) -> str:
        """Generate alternate correct responses."""
        correct_option = question['correct_option']
        if correct_option not in ['A', 'B', 'C', 'D']:
            return ''
            
        correct_answer = question[f'option_{correct_option.lower()}']
        alternates = []
        
        # Formal/informal variations
        formal_informal = {
            'Scusi': 'Scusa', 'Scusa': 'Scusi',
            'Mi dispiace': 'Mi spiace',
            'Buongiorno': 'Buon giorno',
        }
        
        if correct_answer in formal_informal:
            alternates.append(formal_informal[correct_answer])
        
        return '; '.join(alternates) if alternates else ''
    
    def validate_question(self, question: Dict, idx: int) -> bool:
        """Validate a question. Now allows any topic at any level."""
        errors = []
        
        # Required fields
        required = ['question_text', 'english_translation', 'option_a', 
                   'option_b', 'option_c', 'option_d', 'correct_option', 
                   'cefr_level', 'topic']
        
        for field in required:
            if not question.get(field):
                errors.append(f"Missing: {field}")
        
        if errors:
            self.validation_errors.append(f"Row {idx}: {'; '.join(errors)}")
            return False
        
        # Validate correct_option
        if question['correct_option'] not in ['A', 'B', 'C', 'D']:
            errors.append(f"Invalid correct_option: {question['correct_option']}")
        
        # Validate CEFR level
        if question['cefr_level'] not in ['A0', 'A1', 'A2', 'B1', 'B2', 'C1', 'C2']:
            errors.append(f"Invalid CEFR: {question['cefr_level']}")
        
        # Validate topic - now checks against ALL topics regardless of level
        current_topic = question['topic']
        
        # Check if it's valid across all topics
        if current_topic not in self.ALL_VALID_TOPICS:
            # Check if it would be auto-corrected (case-insensitive match)
            will_be_corrected = False
            for valid_topic in self.ALL_VALID_TOPICS:
                if current_topic.lower() == valid_topic.lower():
                    will_be_corrected = True
                    if self.verbose:
                        self.warnings.append(f"Row {idx}: Topic '{current_topic}' will be auto-corrected to '{valid_topic}'")
                    break
            
            if not will_be_corrected:
                # Check for close matches
                best_match = None
                best_score = 0
                for valid_topic in self.ALL_VALID_TOPICS:
                    score = self.similarity_ratio(current_topic, valid_topic)
                    if score > best_score:
                        best_score = score
                        best_match = valid_topic
                
                if best_score > 0.9:
                    # Will be auto-corrected
                    if self.verbose:
                        self.warnings.append(f"Row {idx}: Topic '{current_topic}' will be auto-corrected to '{best_match}'")
                elif best_score > 0.85:
                    # Close but won't be auto-corrected
                    level = question.get('cefr_level', 'Unknown')
                    self.warnings.append(f"Row {idx}: Topic '{current_topic}' similar to '{best_match}' - consider manual correction")
                else:
                    # Unknown topic
                    level = question.get('cefr_level', 'Unknown')
                    self.warnings.append(f"Row {idx}: Unknown topic '{current_topic}' at {level}")
        
        # Check question format
        qt = question['question_text']
        if not qt.startswith('Write:') and '___' not in qt and ':' not in qt:
            self.warnings.append(f"Row {idx}: No clear blank or prompt")
        
        # Check options
        options = [question['option_a'], question['option_b'], 
                  question['option_c'], question['option_d']]
        if len(set(options)) != 4:
            errors.append("Duplicate options")
        
        # Italian-specific checks
        self.validate_italian_specifics(question, idx)
        
        if errors:
            self.validation_errors.append(f"Row {idx}: {'; '.join(errors)}")
            return False
        
        return True
    
    def validate_italian_specifics(self, question: Dict, idx: int):
        """Check Italian-specific issues."""
        cs = question.get('complete_sentence', '').lower()
        eng = question.get('english_translation', '').lower()
        
        # Weekday translations
        weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        for day in weekdays:
            if f"on {day}" in eng and "il " in cs and f"{day}s" not in eng:
                self.warnings.append(f"Row {idx}: 'Il {day}' → 'On {day}s' (habitual)")
        
        # Uncontracted articles
        uncontracted = [
            (r'\bdi\s+il\b', 'del'), (r'\ba\s+il\b', 'al'),
            (r'\bin\s+il\b', 'nel'), (r'\bsu\s+il\b', 'sul'),
        ]
        
        for pattern, correct in uncontracted:
            if re.search(pattern, cs):
                self.warnings.append(f"Row {idx}: Use '{correct}' not separated")
    
    def is_duplicate(self, question: Dict, existing: List[Dict]) -> bool:
        """Check if question is duplicate."""
        for exist in existing:
            # Exact match
            if question['question_text'] == exist['question_text']:
                return True
            
            # Complete sentence match
            if question['complete_sentence'] == exist['complete_sentence']:
                return True
            
            # High similarity
            if (self.similarity_ratio(question['question_text'], exist['question_text']) > 0.9 and
                question['topic'] == exist['topic']):
                return True
        
        return False
    
    def similarity_ratio(self, str1: str, str2: str) -> float:
        """Calculate string similarity."""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def write_output_csv(self, questions: List[Dict], output_file: str):
        """Write questions to CSV."""
        fieldnames = ['complete_sentence', 'question_text', 'english_translation', 
                     'hint', 'alternate_correct_responses', 'option_a', 'option_b', 
                     'option_c', 'option_d', 'correct_option', 'cefr_level', 
                     'topic', 'explanation', 'resource']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for q in questions:
                row = {field: q.get(field, '') for field in fieldnames}
                writer.writerow(row)
    
    def generate_report(self) -> Dict:
        """Generate conversion report."""
        return {
            'statistics': self.stats,
            'validation_errors': self.validation_errors[:10],
            'warnings': self.warnings[:10],
            'duplicates': self.duplicates[:10],
            'auto_corrections': self.auto_corrections[:10],  # Add auto-corrections
            'summary': {
                'success_rate': (self.stats['valid_questions'] / 
                               max(self.stats['total_processed'], 1) * 100),
                'total_errors': len(self.validation_errors),
                'total_warnings': len(self.warnings),
                'total_duplicates': self.stats['duplicates_found'],
                'total_auto_corrections': self.stats['topics_auto_corrected'],  # Add count
                'difficult_topics_ratio': (self.stats['difficult_topics_found'] / 
                                          max(self.stats['valid_questions'], 1) * 100)
            }
        }
    
    def print_report(self, report: Dict):
        """Print formatted report."""
        print("\n" + "="*60)
        print("ITALIAN CEFR CSV CHECK REPORT")
        print("="*60)
        
        print("\n📊 STATISTICS:")
        print(f"  Total Processed: {report['statistics']['total_processed']}")
        print(f"  Valid Questions: {report['statistics']['valid_questions']}")
        print(f"  Invalid Questions: {report['statistics']['invalid_questions']}")
        print(f"  Duplicates Found: {report['statistics']['duplicates_found']}")
        print(f"  Questions Enhanced: {report['statistics']['questions_enhanced']}")
        print(f"  Topics Auto-Corrected: {report['statistics']['topics_auto_corrected']}")
        
        print(f"\n✅ Success Rate: {report['summary']['success_rate']:.1f}%")
        print(f"📚 Difficult Topics: {report['summary']['difficult_topics_ratio']:.1f}%")
        
        # Show auto-corrections if any
        if report.get('auto_corrections') and report['auto_corrections']:
            print(f"\n🔧 AUTO-CORRECTIONS (first 10 of {report['summary']['total_auto_corrections']}):")
            for correction in report['auto_corrections']:
                print(f"  • {correction}")
        
        # Show topic mapping summary if mappings were used
        if self.topic_mapping_usage:
            print(f"\n🔄 LEGACY TOPIC MAPPINGS USED:")
            # Show top 5 most common mappings
            sorted_mappings = sorted(self.topic_mapping_usage.items(), key=lambda x: x[1], reverse=True)
            for mapping, count in sorted_mappings[:5]:
                print(f"  • {count}x: {mapping}")
            if len(sorted_mappings) > 5:
                total_mappings = sum(self.topic_mapping_usage.values())
                print(f"  ... and {len(sorted_mappings) - 5} more mappings (total: {total_mappings} questions)")
        
        # Show unmapped topics
        if self.unmapped_topics:
            print(f"\n❗ UNMAPPED TOPICS (need manual attention):")
            # Sort by count (most frequent first)
            sorted_unmapped = sorted(self.unmapped_topics.items(), key=lambda x: x[1]['count'], reverse=True)
            
            for topic_key, info in sorted_unmapped:
                if info['best_match'] and info['score'] > 0.5:
                    print(f"  • {topic_key} ({info['count']}x)")
                    print(f"    Suggested: '{info['best_match']}' ({info['score']:.1%} similar)")
                else:
                    print(f"  • {topic_key} ({info['count']}x) - No good match found")
            
            print(f"\n  💡 To fix these:")
            print(f"     1. Add mappings to LEGACY_TOPIC_MAPPINGS in the script")
            print(f"     2. Or rename topics in your CSV to match standard names")
        
        if report['validation_errors']:
            print(f"\n❌ ERRORS (first 10 of {report['summary']['total_errors']}):")
            for error in report['validation_errors']:
                print(f"  • {error}")
        
        if report['warnings']:
            print(f"\n⚠️ WARNINGS (first 10 of {report['summary']['total_warnings']}):")
            for warning in report['warnings']:
                print(f"  • {warning}")
        
        if report['duplicates']:
            print(f"\n🔄 DUPLICATES (first 10 of {report['summary']['total_duplicates']}):")
            for idx, text in report['duplicates']:
                print(f"  • Row {idx}: {text[:60]}...")
        
        print("\n" + "="*60)
        print("\nNOTE: Topics can now appear at any CEFR level.")
        print("This allows flexibility in question difficulty while maintaining topic consistency.")
        print("="*60)
    
    def save_full_report(self, report: Dict, filename: str = "report.txt"):
        """Save complete report to file."""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("COMPLETE ITALIAN CEFR CONVERSION REPORT\n")
            f.write("="*60 + "\n\n")
            
            f.write("STATISTICS:\n")
            for key, value in report['statistics'].items():
                f.write(f"  {key}: {value}\n")
            
            f.write(f"\nSuccess Rate: {report['summary']['success_rate']:.1f}%\n")
            
            f.write("\n" + "="*60 + "\n")
            f.write("CONFIGURATION NOTE:\n")
            f.write("Topics are now unified across all CEFR levels.\n")
            f.write("Any topic can appear at any level, allowing flexibility in question difficulty.\n")
            f.write("="*60 + "\n\n")
            
            # Add full mapping report if mappings were used
            if self.topic_mapping_usage:
                f.write("\n" + "="*60 + "\n")
                f.write(self.generate_mapping_report())
                f.write("\n" + "="*60 + "\n\n")
            
            # Add unmapped topics report
            if self.unmapped_topics:
                f.write("\n" + "="*60 + "\n")
                f.write("UNMAPPED TOPICS REPORT\n")
                f.write("="*60 + "\n")
                f.write("These topics could not be automatically mapped and need manual attention:\n\n")
                
                sorted_unmapped = sorted(self.unmapped_topics.items(), key=lambda x: x[1]['count'], reverse=True)
                
                for topic_key, info in sorted_unmapped:
                    f.write(f"{topic_key}: {info['count']} occurrences\n")
                    if info['best_match'] and info['score'] > 0.5:
                        f.write(f"  Suggested mapping: '{info['best_match']}' ({info['score']:.1%} similar)\n")
                    else:
                        f.write(f"  No suitable match found\n")
                    f.write("\n")
                
                f.write("\nTo add mappings for these topics, update LEGACY_TOPIC_MAPPINGS in the script.\n")
                f.write("="*60 + "\n\n")
            
            if self.validation_errors:
                f.write(f"\nALL VALIDATION ERRORS ({len(self.validation_errors)}):\n")
                for error in self.validation_errors:
                    f.write(f"{error}\n")
            
            if self.warnings:
                f.write(f"\nALL WARNINGS ({len(self.warnings)}):\n")
                for warning in self.warnings:
                    f.write(f"{warning}\n")
            
            if self.duplicates:
                f.write(f"\nALL DUPLICATES ({len(self.duplicates)}):\n")
                for idx, text in self.duplicates:
                    f.write(f"Row {idx}: {text}\n")
            
            if self.auto_corrections:
                f.write(f"\nALL AUTO-CORRECTIONS ({len(self.auto_corrections)}):\n")
                for correction in self.auto_corrections:
                    f.write(f"{correction}\n")


def deduplicate_csv(input_file: str, output_file: str, threshold: float = 0.85):
    """Remove duplicates from CSV based on similarity threshold."""
    converter = ItalianCEFRQuestionConverter()
    
    # Read CSV
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except:
        df = pd.read_csv(input_file, encoding='latin-1')
    
    print(f"Checking {len(df)} questions for duplicates (threshold: {threshold:.0%})...")
    
    questions_to_keep = []
    duplicates_found = []
    
    for idx, row in df.iterrows():
        question = converter.convert_question(row, idx)
        if not question:
            continue
        
        is_duplicate = False
        for kept_q in questions_to_keep:
            text_sim = converter.similarity_ratio(
                question['question_text'], 
                kept_q['question_text']
            )
            
            if text_sim > threshold and question['topic'] == kept_q['topic']:
                is_duplicate = True
                duplicates_found.append((idx, question['question_text']))
                break
        
        if not is_duplicate:
            questions_to_keep.append(question)
    
    print(f"Found {len(duplicates_found)} duplicates")
    print(f"Keeping {len(questions_to_keep)} unique questions")
    
    if questions_to_keep:
        converter.write_output_csv(questions_to_keep, output_file)
        print(f"✅ Wrote deduplicated questions to {output_file}")
    
    return len(questions_to_keep), len(duplicates_found)


def clean_database(db_path: str, backup: bool = True, threshold: float = 0.85):
    """Clean duplicates from SQLite database."""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    if backup:
        backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Export to check
    cursor.execute("""
        SELECT id, question_text, complete_sentence, topic, cefr_level
        FROM questions
        ORDER BY cefr_level, topic, id
    """)
    
    rows = cursor.fetchall()
    print(f"Checking {len(rows)} questions in database...")
    
    converter = ItalianCEFRQuestionConverter()
    to_remove = []
    seen = []
    
    for row in rows:
        q_id, q_text, q_complete, q_topic, q_level = row
        
        is_duplicate = False
        for seen_q in seen:
            text_sim = converter.similarity_ratio(q_text, seen_q['text'])
            complete_sim = converter.similarity_ratio(q_complete or '', seen_q['complete'] or '')
            
            if (text_sim > threshold or complete_sim > threshold) and q_topic == seen_q['topic']:
                is_duplicate = True
                to_remove.append(q_id)
                print(f"  Duplicate found: ID {q_id}")
                break
        
        if not is_duplicate:
            seen.append({
                'id': q_id,
                'text': q_text,
                'complete': q_complete,
                'topic': q_topic
            })
    
    if to_remove:
        print(f"\nFound {len(to_remove)} duplicates to remove")
        response = input("Remove these from database? (yes/no): ")
        
        if response.lower() == 'yes':
            cursor.execute("BEGIN TRANSACTION")
            try:
                for q_id in to_remove:
                    cursor.execute("DELETE FROM questions WHERE id = ?", (q_id,))
                    cursor.execute("DELETE FROM enhanced_performance WHERE question_id = ?", (q_id,))
                conn.commit()
                print(f"✅ Removed {len(to_remove)} questions")
            except Exception as e:
                conn.rollback()
                print(f"❌ Error: {e}")
    else:
        print("✅ No duplicates found")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Italian CEFR Question CSV Checker and Converter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s check input.csv                    # Validate only
  %(prog)s convert input.csv output.csv       # Convert and enhance
  %(prog)s dedupe input.csv output.csv        # Remove duplicates
  %(prog)s clean-db database.db --backup      # Clean database
  
  %(prog)s convert input.csv output.csv --no-enhance  # Convert without enhancements
  %(prog)s dedupe input.csv output.csv --threshold 0.9  # Stricter duplicate detection
        """
    )
    
    parser.add_argument('command', choices=['check', 'convert', 'dedupe', 'clean-db'],
                       help='Operation to perform')
    parser.add_argument('input', help='Input CSV file or database')
    parser.add_argument('output', nargs='?', help='Output CSV file')
    parser.add_argument('--threshold', type=float, default=0.85,
                       help='Similarity threshold for duplicates (0.0-1.0)')
    parser.add_argument('--no-enhance', action='store_true',
                       help='Skip enhancement step')
    parser.add_argument('--backup', action='store_true',
                       help='Create backup before cleaning database')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress')
    parser.add_argument('--report', help='Save full report to file')
    
    args = parser.parse_args()
    
    if args.command == 'check':
        print(f"Checking {args.input}...")
        converter = ItalianCEFRQuestionConverter(verbose=args.verbose)
        report = converter.convert_csv(
            args.input,
            validate_only=True
        )
        converter.print_report(report)
        
        if args.report:
            converter.save_full_report(report, args.report)
            print(f"Full report saved to {args.report}")
    
    elif args.command == 'convert':
        if not args.output:
            print("❌ Output file required for convert command")
            sys.exit(1)
        
        print(f"Converting {args.input} → {args.output}")
        converter = ItalianCEFRQuestionConverter(verbose=args.verbose)
        report = converter.convert_csv(
            args.input,
            args.output,
            enhance_questions=not args.no_enhance
        )
        converter.print_report(report)
        
        if args.report:
            converter.save_full_report(report, args.report)
    
    elif args.command == 'dedupe':
        if not args.output:
            print("❌ Output file required for dedupe command")
            sys.exit(1)
        
        kept, removed = deduplicate_csv(args.input, args.output, args.threshold)
        print(f"\n✅ Complete: {kept} unique questions, {removed} duplicates removed")
    
    elif args.command == 'clean-db':
        clean_database(args.input, args.backup, args.threshold)


if __name__ == "__main__":
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        print("Italian CEFR CSV Checker - Quick Start\n")
        print("Usage examples:")
        print("  python italian_csv_checker.py check your_questions.csv")
        print("  python italian_csv_checker.py convert input.csv output.csv")
        print("  python italian_csv_checker.py dedupe input.csv cleaned.csv")
        print("\nFor full help: python italian_csv_checker.py --help")
    else:
        main()