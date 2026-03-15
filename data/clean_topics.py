#!/usr/bin/env python3
"""
clean_topics.py

Merges variant topic names in Italian_XX.csv files into canonical names
that match TOPICS_BY_LEVEL in make_italian_datasets_v2.py.

Usage:
    python3 clean_topics.py --level A2 --dry-run   # preview changes
    python3 clean_topics.py --level A2              # apply changes
    python3 clean_topics.py --all                   # clean all levels
    python3 clean_topics.py --level A2 --report     # just show topic counts
"""

import argparse
import csv
import shutil
from collections import Counter
from pathlib import Path
from datetime import datetime

OUTDIR = Path(".")

# ---------------------------------------------------------------------------
# Canonical topic lists (must match TOPICS_BY_LEVEL in make_italian_datasets)
# ---------------------------------------------------------------------------
CANONICAL_TOPICS = {
    "A1": [
        "Presente indicativo: verbi regolari -are",
        "Presente indicativo: verbi regolari -ere",
        "Presente indicativo: verbi regolari -ire",
        "Presente indicativo: verbi regolari -ire con -isc- (tipo finire)",
        "Verbo essere (to be)",
        "Verbo avere (to have)",
        "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)",
        "Verbi modali (uso base: potere, dovere, volere)",
        "Piacere (uso base: piace/piacciono)",
        "Verbi riflessivi (presente)",
        "Forme negative semplici",
        "Parole interrogative",
        "C'è / Ci sono",
        "Congiunzioni base (e, ma, o, anche, perché)",
        "Preposizioni semplici (uso base)",
        "Articoli determinativi e indeterminativi",
        "Nomi: genere e numero",
        "Aggettivi: accordo base",
        "Aggettivi possessivi (base)",
        "Dimostrativi (base)",
        "Pronomi soggetto",
        "Molto/Poco (quantificatori e avverbi base)",
        "Avverbi di modo base (bene, male, velocemente, lentamente)",
        "Giorni della settimana",
        "Mesi",
        "Stagioni",
        "Orario (telling time)",
        "Espressioni di tempo basilari",
        "Numeri cardinali 0–1000+ e prezzi",
        "Date (esprimere la data)",
        "Saluti, presentazioni, tu/Lei",
        "Direzioni e luoghi in città",
        "Colori",
        "Famiglia",
        "Cibo e bevande (base)",
        "Abbigliamento",
        "Casa (stanze e oggetti)",
        "Corpo (base)",
        "Trasporti (base)",
        "Professioni (base)",
        "Routine quotidiana",
        "Animali (comuni)",
        "Ristorante (frasi base)",
        "Geografia: paesi, città, nazionalità",
        "Meteo (base)",
        "Emozioni e stati d'animo (base)",
        "Telefono e comunicazione (base)",
        "Espressioni comuni",
    ],
    "A2": [
        "Verbo Essere e Presente Indicativo",
        "Presente indicativo: verbi regolari -are (A2)",
        "Presente indicativo: verbi regolari -ere (A2)",
        "Presente indicativo: verbi regolari -ire (A2)",
        "Preposizioni Semplici",
        "Articoli partitivi",
        "Preposizioni articolate",
        "Confronto articoli (def/indef/partitivi)",
        "Verbi riflessivi (presente e passato prossimo)",
        "Passato prossimo (reg/irr; essere/avere; accordo)",
        "Participio Passato (comuni; irregolari frequenti)",
        "Imperfetto (introduzione)",
        "Imperfetto vs. Passato Prossimo",
        "Futuro semplice (base)",
        "Stare per + infinito",
        "Condizionale Presente",
        "Stare + Gerundio",
        "Verbi modali + infinito",
        "Piacere (con pronomi; passato)",
        "Volerci vs. Metterci",
        "Imperativo (tu/noi/voi; base)",
        "Imperativo Formale",
        "Pronomi diretti",
        "Pronomi indiretti",
        "Pronomi Combinati",
        "Pronomi Relativi",
        "Aggettivi e Pronomi Indefiniti",
        "Particella ne (base)",
        "Particella Ci",
        "Si Impersonale",
        "Avverbi di frequenza/tempo/luogo",
        "Comparativi e Superlativi",
        "Forme negative composte (non...mai, non...niente, ecc.)",
        "Congiunzioni e connettivi (base)",
        "Espressioni di tempo (fa, da, tra/fra; ore)",
        "Numeri oltre 100",
        "Numeri ordinali",
        "Forme speciali di bello, buono, grande (pre-nominali)",
        "Plurali irregolari (nomi con plurale irregolare)",
        "Shopping (vocabolario)",
        "Viaggi (trasporti, biglietti)",
        "Casa e Quartiere (vocabolario)",
        "Lavoro/ufficio (base)",
        "Scuola/Università (base)",
        "Salute (base)",
        "Ristorante (menu/prenotare/conti)",
        "Routine Quotidiana Dettagliata (vocabolario)",
        "Tempo (weather) – esteso",
        "Ricordi (narrazione semplice)",
        "Programmi (piani futuri, inviti)",
        "Descrizioni Fisiche e del Carattere (vocabolario)",
        "Prestare e chiedere in prestito",
        "Sport e Hobby (vocabolario)",
        "Sentire (hear vs. smell vs. feel)",
    ],
    "B1": [
        "Imperfetto vs Passato prossimo",
        "Trapassato prossimo",
        "Futuro semplice (esteso)",
        "Condizionale presente",
        "Congiuntivo presente (introduzione: opinioni, emozioni base)",
        "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
        "Periodo ipotetico I tipo",
        "Stare + gerundio (progressivo)",
        "Gerundio (usi)",
        "Imperativo (pronomi atoni; negazione)",
        "Pronomi combinati (glielo, me ne, ecc.)",
        "Ci e ne (avanzato)",
        "Pronomi relativi (che/cui; prep + cui)",
        "Pronomi indefiniti (comuni)",
        "Passivo con essere (tempi principali)",
        "Si impersonale / si passivante (base)",
        "Discorso indiretto (base)",
        "Connettivi di causa/effetto/concessione/ordine",
        "Comparativi e superlativi (irregolarità)",
        "Verbi modali + pronomi clitici",
        "Sapere vs. Conoscere",
        "Preposizioni + infinito (da, di, a)",
        "Suffissi (diminutivi, accrescitivi, peggiorativi)",
        "Pronomi possessivi (vs aggettivi possessivi)",
        "Infinito passato (dopo aver/essere + participio)",
        "Verbi impersonali",
        "Lavoro",
        "Viaggi (reclami/imprevisti)",
        "Sport e hobby",
        "Tecnologia (uso quotidiano)",
        "Casa e quartiere (problemi/soluzioni)",
        "Relazioni",
        "Ambiente (pratiche)",
        "Ristorante (recensioni/preferenze)",
        "Salute e medicina (sintomi, consigli, visite mediche)",
        "Media e attualità (notizie base, opinioni)",
        "Servizi (banca, posta, reclami)",
        "Istruzione (esperienze scolastiche, esami)",
        "Ricordi (narrazione estesa)",
        "Descrizioni (dettaglio)",
        "Animali domestici e veterinario",
        "Proprietà e confini",
        "Prestiti e restituzione (avanzato)",
        "Sentimenti (vocabolario)",
        "Gusti (vocabolario)",
        "Professioni (vocabolario)",
        "Musica e cinema (vocabolario)",
        "Verbi causativi (introduzione)",
        "Burocrazia (base)",
        "Cucina (base)",
        "Business e e-commerce (introduzione)",
        "Memoria e cambiamento",
        "Decisioni",
        "Comunicazione efficace e influenza sociale",
    ],
    "B2": [
        "Congiuntivo presente",
        "Congiuntivo passato",
        "Congiuntivo imperfetto",
        "Congiuntivo trapassato",
        "Concordanza dei tempi (casi tipici)",
        "Periodo ipotetico II e III",
        "Condizionale passato",
        "Futuro anteriore",
        "Forme implicite (infinito, gerundio, participio con valore temporale/causale)",
        "Passivo con essere/venire (avanzato)",
        "Si passivante (avanzato)",
        "Verbi causativi (fare + infinito)",
        "Verbi causativi: lasciare (permettere)",
        "Verbi fraseologici (stare per, finire per, ecc.)",
        "Pronomi relativi avanzati (il quale; cui articolate)",
        "Pronomi indefiniti/dimostrativi (avanzato)",
        "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)",
        "Costruzioni con gerundio/participio",
        "Preposizioni complesse e locuzioni",
        "Registro e toni (formale/informale)",
        "Connettivi complessi (benché, sebbene, purché, ecc.)",
        "Verbi pronominali (andarsene, cavarsela, farcela, ecc.)",
        "Discorso indiretto (avanzato)",
        "Espressioni idiomatiche (comuni)",
        "Linguaggio accademico",
        "Ricerca e analisi",
        "Lessico metaforico e figurato",
        "Passato remoto (introduzione)",
        "Finanza personale (banca, prestiti, investimenti, pensione)",
        "Assicurazioni (salute, auto, casa, vita)",
        "Immobili (comprare, affittare, mutui, contratti)",
        "Automobile (manutenzione, riparazioni, assicurazioni, guasti)",
        "Salute avanzata (procedure mediche, malattie croniche, specialisti, salute mentale)",
        "Cucina e gastronomia (ricette, tecniche, ingredienti, vino)",
        "Questioni legali personali (contratti, diritti consumatori, cause)",
        "Casa e giardinaggio (fai-da-te, piante, ristrutturazioni, elettrodomestici)",
        "Fitness e benessere (palestra, allenamento, nutrizione)",
        "Animali domestici - cure avanzate e problemi",
        "Gestione delle conversazioni difficili",
        "Conflitto e risoluzione (avanzato)",
        "Negoziazione e persuasione",
        "Riconoscere e rispondere a manipolazioni",
        "Carattere e personalità (avanzato)",
        "Cultura",
        "Politica e diritto (lessico generale)",
        "Economia e finanza (lessico generale)",
        "Sanità e società",
        "Ambiente (dibattito)",
        "Tecnologia (privacy/AI/social)",
        "Business & e-commerce",
        "Trasporti (norme/sostenibilità)",
        "Professioni (carriere)",
        "Burocrazia (pratiche)",
    ],
    "C1": [
        "Congiuntivo vs indicativo (scelte stilistiche)",
        "Concordanza dei tempi (casi complessi)",
        "Discorso indiretto avanzato (deissi/tempi)",
        "Participio passato assoluto; costruzioni assolute",
        "Stile inverso e focalizzazioni",
        "Passato remoto (uso letterario/storico)",
        "Trapassato remoto (ricettivo)",
        "Andare + participio (valore di dovere)",
        "Si impersonale/passivante (sfumature/ambiguità)",
        "Nominalizzazioni e densità informativa",
        "Collocazioni e fraseologia",
        "Lessico formale/accademico",
        "Espressioni idiomatiche e proverbi",
        "Connettivi formali e marcatori discorsivi",
        "Lessico legale",
        "Lessico business",
        "Lessico medico/sanitario",
        "Lessico tecnologico",
        "Lessico agricolo",
        "Lessico enologico",
        "Lessico figurato/metaforico",
        "Filosofia e pensiero critico",
        "Ricerca accademica",
        "Negoziazione e diplomazia",
        "Cultura (analisi/commento)",
        "Società (valori/demografia)",
        "Politica e diritto (argomentazione)",
        "Economia e finanza (analisi)",
        "Ambiente (policy/dibattito)",
        "Urbanistica e trasporti",
        "Burocrazia (procedure avanzate)",
        "Professioni (settoriale)",
        "Memoria e cambiamento (saggi)",
        "Sentimenti e stati d'animo (lessico fine)",
        "Decisioni (pro/contro)",
        "Musica e cinema (recensioni formali)",
        "Educazione (accademia, università)",
        "Business & e-commerce (strategie)",
        "Registro colloquiale e gergale",
        "Pronomi relativi complessi (il cui, colui che, ecc.)",
    ],
}

# ---------------------------------------------------------------------------
# Merge maps: variant name → canonical name
# None = delete the row entirely
# ---------------------------------------------------------------------------
MERGE_MAP = {
    "A1": {
        # Article variants
        "Articoli determinativi":                               "Articoli determinativi e indeterminativi",
        "Articoli indeterminativi":                             "Articoli determinativi e indeterminativi",
        "Articoli (generale)":                                  "Articoli determinativi e indeterminativi",
        # Grammar variants
        "Dimostrativi":                                         "Dimostrativi (base)",
        "Nomi (genere e numero)":                               "Nomi: genere e numero",
        "Verbi modali (potere, dovere, volere)":                "Verbi modali (uso base: potere, dovere, volere)",
        "Verbi irregolari":                                     "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)",
        "Aggettivi possessivi":                                 "Aggettivi possessivi (base)",
        "Aggettivi (accordo e uso)":                            "Aggettivi: accordo base",
        "Espressioni di tempo":                                 "Espressioni di tempo basilari",
        "Preposizioni semplici":                                "Preposizioni semplici (uso base)",
        "Presente indicativo: verbi regolari -ire con -isc-":   "Presente indicativo: verbi regolari -ire con -isc- (tipo finire)",
        "Parole interrogative":                                 "Parole interrogative",
        # Reflexive verbs — keep in A1 as present tense only
        "Verbi riflessivi":                                     "Verbi riflessivi (presente)",
        # Vocabulary variants
        "Trasporti (vocabolario)":                              "Trasporti (base)",
        "Corpo (vocabolario)":                                  "Corpo (base)",
        "Giorni della settimana (vocabolario)":                 "Giorni della settimana",
        "Saluti e presentazioni":                               "Saluti, presentazioni, tu/Lei",
        "Professioni (vocabolario)":                            "Professioni (base)",
        "Stagioni (vocabolario)":                               "Stagioni",
        "Animali (vocabolario)":                                "Animali (comuni)",
        "Geografia (paesi, città, nazionalità)":                "Geografia: paesi, città, nazionalità",
        "Numeri e prezzi (0-100)":                              "Numeri cardinali 0–1000+ e prezzi",
        "Numeri 0–100 e prezzi":                                "Numeri cardinali 0–1000+ e prezzi",
        "Meteo (vocabolario)":                                  "Meteo (base)",
        "Famiglia (vocabolario)":                               "Famiglia",
        "Colori (vocabolario)":                                 "Colori",
        "Abbigliamento (vocabolario)":                          "Abbigliamento",
        "Cibo e bevande (vocabolario)":                         "Cibo e bevande (base)",
        "Ristorante (frasi e vocabolario)":                     "Ristorante (frasi base)",
        "Casa (vocabolario)":                                   "Casa (stanze e oggetti)",
        "Mesi (vocabolario)":                                   "Mesi",
        # DELETE — wrong level or not a language topic
        "Si impersonale":                                       None,  # DELETE — A2 topic
        "Articoli partitivi":                                   None,  # DELETE — A2 topic
        "Cultura generale":                                     None,  # DELETE — not a language topic
    },
    "A2": {
        # Capitalisation/punctuation variants
        "Articoli Partitivi":                                   "Articoli partitivi",
        "Preposizioni Articolate":                              "Preposizioni articolate",
        "Confronto Articoli":                                   "Confronto articoli (def/indef/partitivi)",
        "Verbi Riflessivi (presente e passato prossimo)":       "Verbi riflessivi (presente e passato prossimo)",
        "Participio passato (comuni; irregolari frequenti)":    "Participio Passato (comuni; irregolari frequenti)",
        "Participio Passato (irregolari frequenti)":            "Participio Passato (comuni; irregolari frequenti)",
        "Imperfetto":                                           "Imperfetto (introduzione)",
        "Imperfetto vs Passato Prossimo":                       "Imperfetto vs. Passato Prossimo",
        "Futuro Semplice":                                      "Futuro semplice (base)",
        "Stare per + Infinito":                                 "Stare per + infinito",
        "Verbi Modali (+ infinito)":                            "Verbi modali + infinito",
        "Piacere (con pronomi, passato)":                       "Piacere (con pronomi; passato)",
        "Volerci vs Metterci":                                  "Volerci vs. Metterci",
        "Imperativo (tu/noi/voi)":                              "Imperativo (tu/noi/voi; base)",
        "Pronomi Diretti":                                      "Pronomi diretti",
        "Pronomi Indiretti":                                    "Pronomi indiretti",
        "Particella ci (base)":                                 "Particella Ci",
        "Particella Ne":                                        "Particella ne (base)",
        "Avverbi di Frequenza/Tempo/Luogo":                     "Avverbi di frequenza/tempo/luogo",
        "Comparativi e superlativi (base)":                     "Comparativi e Superlativi",
        "Espressioni di Tempo (fa, da, tra/fra)":               "Espressioni di tempo (fa, da, tra/fra; ore)",
        "Numeri Oltre 100 (vocabolario)":                       "Numeri oltre 100",
        "Passato Prossimo (essere/avere, accordo)":             "Passato prossimo (reg/irr; essere/avere; accordo)",
        "Partcipazione Passato (irregolari frequenti)":         "Participio Passato (comuni; irregolari frequenti)",
        # (vocabolario) suffix variants
        "Shopping":                                             "Shopping (vocabolario)",
        "Viaggi (trasporti, biglietti) (vocabolario)":          "Viaggi (trasporti, biglietti)",
        "Casa e quartiere":                                     "Casa e Quartiere (vocabolario)",
        "Lavoro/Ufficio (vocabolario)":                         "Lavoro/ufficio (base)",
        "Scuola e Università (vocabolario)":                    "Scuola/Università (base)",
        "Salute (vocabolario)":                                 "Salute (base)",
        "Ristorante (menu/prenotare/conti) (vocabolario)":      "Ristorante (menu/prenotare/conti)",
        "Routine quotidiana dettagliata":                       "Routine Quotidiana Dettagliata (vocabolario)",
        "Tempo/Meteo (vocabolario)":                            "Tempo (weather) – esteso",
        "Descrizioni fisiche e del carattere":                  "Descrizioni Fisiche e del Carattere (vocabolario)",
        # Renamed to match new canonical names
        "Presente Indicativo: Verbi Regolari -are":             "Presente indicativo: verbi regolari -are (A2)",
        "Presente Indicativo: Verbi Regolari -ere":             "Presente indicativo: verbi regolari -ere (A2)",
        "Presente Indicativo: Verbi Regolari -ire":             "Presente indicativo: verbi regolari -ire (A2)",
        "Sentire (hear vs smell vs feel)":                      "Sentire (hear vs. smell vs. feel)",
        "Sport e Hobby (vocabolario)":                          "Sport e Hobby (vocabolario)",
        # DELETE — A1 topics that leaked in
        "Parole Interrogative":                                 None,  # DELETE — A1 topic
        "Aggettivi Dimostrativi":                               None,  # DELETE — A1 topic
        "Aggettivi Possessivi":                                 None,  # DELETE — A1 topic
        "Famiglia (vocabolario)":                               None,  # DELETE — A1 topic
        "Orario (vocabolario)":                                 None,  # DELETE — A1 topic
    },
    "B1": {
        # Grammar duplicates → canonical
        "Accordo del participio passato":                       "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
        "Participio passato":                                   "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
        "Si impersonale/si passivante":                         "Si impersonale / si passivante (base)",
        "Discorso indiretto":                                   "Discorso indiretto (base)",
        "Passivo (con essere)":                                 "Passivo con essere (tempi principali)",
        "Pronomi combinati (glielo, me ne)":                    "Pronomi combinati (glielo, me ne, ecc.)",
        "Pronomi indefiniti":                                   "Pronomi indefiniti (comuni)",
        "Periodo ipotetico (I tipo)":                           "Periodo ipotetico I tipo",
        "Futuro semplice":                                      "Futuro semplice (esteso)",
        "Pronomi relativi (che/cui)":                           "Pronomi relativi (che/cui; prep + cui)",
        "Imperativo (pronomi atoni, negazione)":                "Imperativo (pronomi atoni; negazione)",
        "Connettivi (causa/effetto/concessione)":               "Connettivi di causa/effetto/concessione/ordine",
        "Connettivi di causa/effetto/concessione/ordine)":      "Connettivi di causa/effetto/concessione/ordine",
        "Salute e medicina (sintomi, consigli)":                "Salute e medicina (sintomi, consigli, visite mediche)",
        "Media e attualità":                                    "Media e attualità (notizie base, opinioni)",
        "Ambiente (vocabolario e pratiche)":                    "Ambiente (pratiche)",
        "Sport e hobby (vocabolario)":                          "Sport e hobby",
        "Hobby (vocabolario)":                                  "Sport e hobby",
        "Lavoro (vocabolario e frasi)":                         "Lavoro",
        "Congiuntivo presente":                                 "Congiuntivo presente (introduzione: opinioni, emozioni base)",
        "Educazione (vocabolario)":                             "Istruzione (esperienze scolastiche, esami)",
        # Music + Cinema → merged canonical
        "Musica (vocabolario)":                                 "Musica e cinema (vocabolario)",
        "Cinema (vocabolario)":                                 "Musica e cinema (vocabolario)",
        # Borderline B1/B2 → renamed as introductory
        "Verbi causativi":                                      "Verbi causativi (introduzione)",
        "Burocrazia":                                           "Burocrazia (base)",
        "Cucina (vocabolario)":                                 "Cucina (base)",
        "Business e e-commerce (vocabolario)":                  "Business e e-commerce (introduzione)",
        "Cultura (vocabolario)":                                None,  # DELETE — B2 topic
        # DELETE — wrong level
        "Particella ci":                                        None,  # DELETE — A2
        "Particella ne":                                        None,  # DELETE — A2
        "Pronomi diretti":                                      None,  # DELETE — A2
        "Pronomi indiretti":                                    None,  # DELETE — A2
        "Preposizioni semplici":                                None,  # DELETE — A1
        "Orario (vocabolario)":                                 None,  # DELETE — A1
        "Tempo/Meteo (vocabolario)":                            None,  # DELETE — A1
        "Trasporti (vocabolario)":                              None,  # DELETE — A1
        "Stare per + infinito":                                 None,  # DELETE — A2
        "Shopping (vocabolario)":                               None,  # DELETE — A2
        "Società (vocabolario)":                                None,  # DELETE — B2
    },
    "B2": {
        # Congiuntivo fragments
        "Congiuntivo":                                          "Congiuntivo presente",
        "Congiuntivo (Desiderio)":                              "Congiuntivo presente",
        "Congiuntivo (Verbi Impersonali)":                      "Congiuntivo presente",
        "Congiuntivo (Indefinito)":                             "Congiuntivo presente",
        "Congiuntivo (Espressioni Fisse)":                      "Congiuntivo presente",
        "Congiuntivo (Domanda Indiretta)":                      "Congiuntivo presente",
        "Congiuntivo (Condizionale)":                           "Concordanza dei tempi (casi tipici)",
        "Congiuntivo (Comparativo)":                            "Concordanza dei tempi (casi tipici)",
        "Indicativo vs. Congiuntivo":                           "Concordanza dei tempi (casi tipici)",
        "Congiuntivo (Concessive)":                             "Connettivi complessi (benché, sebbene, purché, ecc.)",
        # Passivo split
        "Passivo con essere/venire; si passivante (avanzato)":  "Passivo con essere/venire (avanzato)",
        "Passivo":                                              "Passivo con essere/venire (avanzato)",
        "Passivo con 'Venire'":                                 "Passivo con essere/venire (avanzato)",
        "Passivo + Preposizioni":                               "Passivo con essere/venire (avanzato)",
        "Si passivante":                                        "Si passivante (avanzato)",
        "Si impersonale":                                       "Si passivante (avanzato)",
        "Si Impersonale (Verbi Riflessivi)":                    "Si passivante (avanzato)",
        "Si Impersonale + Indicativo":                         "Si passivante (avanzato)",
        # Reported speech
        "Discorso indiretto":                                   "Discorso indiretto (avanzato)",
        "Discorso Indiretto (Futuro nel Passato)":              "Discorso indiretto (avanzato)",
        "Verbi Dichiarativi":                                   "Discorso indiretto (avanzato)",
        # Pronouns
        "Pronomi relativi":                                     "Pronomi relativi avanzati (il quale; cui articolate)",
        "Pronomi Relativi + Preposizioni":                      "Pronomi relativi avanzati (il quale; cui articolate)",
        "Pronomi combinati":                                    "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)",
        # Gerund / participle
        "Gerundio Presente":                                    "Costruzioni con gerundio/participio",
        "Gerundio Passato":                                     "Costruzioni con gerundio/participio",
        "Gerundio (Costruzione Assoluta)":                      "Costruzioni con gerundio/participio",
        "Gerundio (Valore Concessivo)":                         "Costruzioni con gerundio/participio",
        "Participio passato":                                   "Costruzioni con gerundio/participio",
        "Participio passato assoluto":                          "Costruzioni con gerundio/participio",
        "Preposizioni + Infinito Passato":                      "Forme implicite (infinito, gerundio, participio con valore temporale/causale)",
        # Connectives / register
        "Connettivi":                                           "Connettivi complessi (benché, sebbene, purché, ecc.)",
        "Lessico formale":                                      "Registro e toni (formale/informale)",
        "Linguagio Formale":                                    "Registro e toni (formale/informale)",
        "Collocazioni":                                         "Registro e toni (formale/informale)",
        "Lessico":                                              "Registro e toni (formale/informale)",
        "Preposizioni":                                         "Preposizioni complesse e locuzioni",
        "Congiuntivo/Preposizioni":                             "Preposizioni complesse e locuzioni",
        # Future
        "Futuro semplice":                                      "Futuro anteriore",
        "Futuro Semplice (Forma Impersonale)":                  "Futuro anteriore",
        "Periodo ipotetico":                                    "Periodo ipotetico II e III",
        # Verbs
        "Verbi pronominali":                                    "Verbi pronominali (andarsene, cavarsela, farcela, ecc.)",
        "Verbi Modali/Fraseologici":                            "Verbi fraseologici (stare per, finire per, ecc.)",
        "Verbi modali":                                         "Verbi fraseologici (stare per, finire per, ecc.)",
        # Vocabulary domains
        "Ambiente":                                             "Ambiente (dibattito)",
        "Tecnologia":                                           "Tecnologia (privacy/AI/social)",
        "Società":                                              "Sanità e società",
        "Sanità":                                               "Sanità e società",
        "Lavoro":                                               "Professioni (carriere)",
        "Lessico (Lavoro)":                                     "Professioni (carriere)",
        "Business":                                             "Business & e-commerce",
        "Lessico (Economia)":                                   "Economia e finanza (lessico generale)",
        "Lessico (Cucina)":                                     "Cucina e gastronomia (ricette, tecniche, ingredienti, vino)",
        "Lessico legale":                                       "Questioni legali personali (contratti, diritti consumatori, cause)",
        "Pubblica Amministrazione":                             "Burocrazia (pratiche)",
        "Cittadinanza":                                         "Burocrazia (pratiche)",
        "Valori":                                               "Carattere e personalità (avanzato)",
        "Demografica":                                          "Sanità e società",
        "Trasporti":                                            "Trasporti (norme/sostenibilità)",
        # New canonical names
        "Espressioni idiomatiche":                              "Espressioni idiomatiche (comuni)",
        "Espressioni idiomatiche e proverbi":                   "Espressioni idiomatiche (comuni)",
        "Linguagio Academico":                                  "Linguaggio accademico",
        "Ricerca":                                              "Ricerca e analisi",
        "Lessico (Metaforico)":                                 "Lessico metaforico e figurato",
        # DELETE — wrong level
        "Passato prossimo":                                     None,  # DELETE — A2
        "Espressioni di tempo basilari":                        None,  # DELETE — A1
        "Verbi riflessivi (presente base)":                     None,  # DELETE — A1
        "Avverbi":                                              None,  # DELETE — too low level
        "Espressioni comuni":                                   None,  # DELETE — A1
        "Indicativo (Futuro Implicito)":                        None,  # DELETE — too vague
        "Educazione":                                           None,  # DELETE — too few questions
        "Urbanistica":                                          None,  # DELETE — C1
    },
    "C1": {
        # ── Congiuntivo fragments → congiuntivo vs indicativo ──────────────
        "Congiuntivo":                                          "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo passato":                                  "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo imperfetto":                               "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Modale)":                                 "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Formale)":                                "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Finale)":                                 "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Concessivo)":                             "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Impersonale)":                            "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Forme Impersonali)":                      "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiuntivo (Verbi di Volontà)":                       "Congiuntivo vs indicativo (scelte stilistiche)",
        "Congiunzioni + Congiuntivo":                           "Congiuntivo vs indicativo (scelte stilistiche)",
        "Gerundio + Congiuntivo":                               "Congiuntivo vs indicativo (scelte stilistiche)",

        # ── Concordanza / period ipotetico ────────────────────────────────
        "Concordanza dei tempi":                                "Concordanza dei tempi (casi complessi)",
        "Periodo ipotetico":                                    "Concordanza dei tempi (casi complessi)",

        # ── Reported speech ───────────────────────────────────────────────
        "Discorso Indiretto (Futuro nel passato)":              "Discorso indiretto avanzato (deissi/tempi)",
        "Discorso indiretto":                                   "Discorso indiretto avanzato (deissi/tempi)",
        "Discorso (Riportare Voci)":                            "Discorso indiretto avanzato (deissi/tempi)",
        "Discorso (Costruzioni Verbali)":                       "Discorso indiretto avanzato (deissi/tempi)",

        # ── Absolute / participio / stile ─────────────────────────────────
        "Costruzioni Assolute":                                 "Participio passato assoluto; costruzioni assolute",
        "Andare + Participio":                                  "Andare + participio (valore di dovere)",
        "Stile Inverso":                                        "Stile inverso e focalizzazioni",
        "Trapassato remoto":                                    "Trapassato remoto (ricettivo)",
        "Passato remoto":                                       "Passato remoto (uso letterario/storico)",

        # ── Passive / impersonal ──────────────────────────────────────────
        "Forme Impersonali":                                    "Si impersonale/passivante (sfumature/ambiguità)",
        "Discorso (Forme Impersonali)":                         "Si impersonale/passivante (sfumature/ambiguità)",
        "Si Impersonale + Riflessivi":                          "Si impersonale/passivante (sfumature/ambiguità)",
        "Forma Passiva":                                        "Si impersonale/passivante (sfumature/ambiguità)",
        "Forma Passiva (con venire)":                           "Si impersonale/passivante (sfumature/ambiguità)",

        # ── Connectives / formal markers ──────────────────────────────────
        "Connettivi":                                           "Connettivi formali e marcatori discorsivi",
        "Connettivi formali":                                   "Connettivi formali e marcatori discorsivi",
        "Congiunzioni (Formali)":                               "Connettivi formali e marcatori discorsivi",
        "Congiunzioni (Concessive)":                            "Connettivi formali e marcatori discorsivi",
        "Congiunzioni (Comparative)":                           "Connettivi formali e marcatori discorsivi",
        "Connettivi di causa/effetto/concessione/ordine)":      "Connettivi formali e marcatori discorsivi",
        "Discorso (Avverbi)":                                   "Connettivi formali e marcatori discorsivi",
        "Discorso (Strutturazione)":                            "Connettivi formali e marcatori discorsivi",

        # ── Formal lexis / register ───────────────────────────────────────
        "Lessico formale":                                      "Lessico formale/accademico",
        "Stile Formale":                                        "Lessico formale/accademico",
        "Espressioni Formali":                                  "Lessico formale/accademico",
        "Verbi (Uso Formale)":                                  "Lessico formale/accademico",
        "Lessico":                                              "Lessico formale/accademico",

        # ── Collocations / phraseology ────────────────────────────────────
        "Lessico (Paronimi)":                                   "Collocazioni e fraseologia",
        "Lessico (Nomi Astratti)":                              "Collocazioni e fraseologia",
        "Lessico (Aggettivi Descrittivi)":                      "Collocazioni e fraseologia",
        "Lessico (Aggettivi Precisi)":                          "Collocazioni e fraseologia",
        "Preposizioni":                                         "Collocazioni e fraseologia",
        "Preposizioni (Figurativo)":                            "Collocazioni e fraseologia",
        "Preposizioni (Uso Idiomatico)":                        "Collocazioni e fraseologia",
        "Pronomi (Verbi Procomplementari)":                     "Collocazioni e fraseologia",
        "Verbi Procomplementari (entrarci)":                    "Collocazioni e fraseologia",
        "Verbi (Riflessivi Idiomatici)":                        "Collocazioni e fraseologia",

        # ── Idioms / proverbs ─────────────────────────────────────────────
        "Espressioni idiomatiche":                              "Espressioni idiomatiche e proverbi",
        "Discorso (Espressioni)":                               "Espressioni idiomatiche e proverbi",
        "Discorso (Frasi Fatte)":                               "Espressioni idiomatiche e proverbi",
        "Discorso (Uso Idiomatico)":                            "Espressioni idiomatiche e proverbi",

        # ── Specialist lexis ──────────────────────────────────────────────
        "Lessico (Culinario)":                                  "Lessico agricolo",

        # ── English-language topics → Italian canonicals ──────────────────
        "Presentation Structure":                               "Connettivi formali e marcatori discorsivi",
        "Academic Discussion":                                  "Ricerca accademica",
        "Professional Consensus":                               "Negoziazione e diplomazia",
        "Sophisticated Discourse Markers":                      "Connettivi formali e marcatori discorsivi",
        "Formal Conversation Management":                       "Connettivi formali e marcatori discorsivi",
        "Advanced Conversation Management":                     "Connettivi formali e marcatori discorsivi",
        "Advanced Argumentation":                               "Filosofia e pensiero critico",
        "Logical Development":                                  "Filosofia e pensiero critico",
        "Sophisticated Disagreement":                           "Filosofia e pensiero critico",
        "Nuanced Expression":                                   "Lessico formale/accademico",

        # ── New canonical names ───────────────────────────────────────────
        "Espressioni Gergali (Slang)":                          "Registro colloquiale e gergale",
        "Pronomi Relativi (Possessivi)":                        "Pronomi relativi complessi (il cui, colui che, ecc.)",
        "Pronomi relativi":                                     "Pronomi relativi complessi (il cui, colui che, ecc.)",

        # ── DELETE — wrong level ──────────────────────────────────────────
        "Aggettivi possessivi (base)":                          None,  # DELETE — A1
        "Tempi Verbali (Passato Prossimo)":                     None,  # DELETE — A2
        "Condizionale (Desiderio)":                             None,  # DELETE — B2
        "Avverbi":                                              None,  # DELETE — too low
        "Infinito":                                             None,  # DELETE — too low
        "Pronomi dimostrativi":                                 None,  # DELETE — A2/B1
        "Pronomi Possessivi":                                   None,  # DELETE — A2/B1
        "Pronomi (Direct)":                                     None,  # DELETE — A2
        "Pronome (Ne Partitivo)":                               None,  # DELETE — A2
        "Discorso (Gestione della Conversazione)":              None,  # DELETE — B2
        "Decisioni (pro/contro)":                               None,  # DELETE — B1 topic leaked in
    },
}

FIELDNAMES = [
    "complete_sentence", "question_text", "english_translation", "hint",
    "alternate_correct_responses", "option_a", "option_b", "option_c", "option_d",
    "correct_option", "cefr_level", "topic", "explanation", "resource",
]


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_csv(level: str):
    path = OUTDIR / f"Italian_{level}.csv"
    if not path.exists():
        print(f"❌  Italian_{level}.csv not found in {OUTDIR.resolve()}")
        return None, None
    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return path, rows


def apply_merge(rows, level: str, dry_run: bool):
    merge_map = MERGE_MAP.get(level, {})
    canonical = set(CANONICAL_TOPICS.get(level, []))

    change_counter: Counter = Counter()
    flagged: Counter = Counter()
    unknown: Counter = Counter()
    kept_rows = []

    for row in rows:
        original = row["topic"]
        if original in merge_map:
            target = merge_map[original]
            if target is None:
                flagged[original] += 1
                continue  # DELETE this row
            else:
                if original != target:
                    change_counter[(original, target)] += 1
                if not dry_run:
                    row["topic"] = target
        elif original not in canonical:
            unknown[original] += 1
        kept_rows.append(row)

    return change_counter, flagged, unknown, kept_rows


def write_csv(path: Path, rows: list):
    backup = path.with_suffix(f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    shutil.copy2(path, backup)
    print(f"  💾 Backup saved to {backup.name}")
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, quoting=csv.QUOTE_ALL,
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"  ✅ Written {len(rows)} rows to {path.name}")


def report_counts(level: str, rows: list):
    counts = Counter(row["topic"] for row in rows)
    canonical = CANONICAL_TOPICS.get(level, [])
    print(f"\n{'Topic':<65} {'Count':>6}")
    print("-" * 75)
    for topic in sorted(counts, key=lambda t: counts[t]):
        marker = "" if topic in canonical else "  ← NOT IN CANONICAL"
        print(f"{topic:<65} {counts[topic]:>6}{marker}")
    print("-" * 75)
    print(f"{'TOTAL':<65} {sum(counts.values()):>6}")
    print(f"\nDistinct topics : {len(counts)}")
    print(f"Canonical topics: {len(canonical)}")
    not_in_csv = [t for t in canonical if t not in counts]
    if not_in_csv:
        print(f"\n⚠️  {len(not_in_csv)} canonical topics have NO questions yet:")
        for t in not_in_csv:
            print(f"   • {t}")


def process_level(level: str, dry_run: bool, report: bool):
    print(f"\n{'='*70}")
    print(f"  Level {level}  {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*70}")

    path, rows = load_csv(level)
    if rows is None:
        return

    if report:
        report_counts(level, rows)
        return

    change_counter, flagged, unknown, rows = apply_merge(rows, level, dry_run)

    if change_counter:
        print(f"\n  📋 Topic renames ({len(change_counter)} distinct mappings):")
        for (old, new), count in sorted(change_counter.items(), key=lambda x: -x[1]):
            print(f"    {count:>4}×  \"{old}\"  →  \"{new}\"")
    else:
        print("\n  ✅ No renames needed.")

    if flagged:
        print(f"\n  🗑️  Deleted topics ({sum(flagged.values())} rows removed):")
        for topic, count in flagged.most_common():
            print(f"    {count:>4}×  \"{topic}\"  → REMOVED")

    if unknown:
        print(f"\n  ❓ Unknown topics (not in merge map or canonical list):")
        for topic, count in unknown.most_common():
            print(f"    {count:>4}×  \"{topic}\"")
        print("     → Add these to MERGE_MAP in this script.")

    total_changes = sum(change_counter.values())
    total_deleted = sum(flagged.values())
    print(f"\n  Total rows   : {len(rows)}")
    print(f"  Rows renamed : {total_changes}")
    print(f"  Rows deleted : {total_deleted}")

    if dry_run:
        print("\n  ℹ️  Dry run — no files modified. Remove --dry-run to apply.")
    else:
        if total_changes > 0 or total_deleted > 0:
            write_csv(path, rows)
        else:
            print("  ℹ️  Nothing to write.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Clean and merge topic names in Italian CSV files")
    ap.add_argument("--level",   type=str, help="Single level: A1, A2, B1, B2, C1")
    ap.add_argument("--all",     action="store_true", help="Process all levels")
    ap.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    ap.add_argument("--report",  action="store_true", help="Just show topic counts, no changes")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not args.level and not args.all:
        print("Specify --level A2 or --all")
    elif args.all:
        for lvl in ["A1", "A2", "B1", "B2", "C1"]:
            process_level(lvl, args.dry_run, args.report)
    else:
        process_level(args.level.upper(), args.dry_run, args.report)
