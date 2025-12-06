#!/usr/bin/env python3
"""
Italian MCQ Dataset Generator - Complete Comprehensive All Levels
Produces high-quality Italian language learning questions with 100% accuracy focus.

COMPLETE COVERAGE:
- A1: 46 topics (40 original + 6 new)
- A2: 48 topics (47 original + 1 new)
- B1: 42 topics (39 original + 3 new)
- B2: 43 topics (29 original + 14 new)
- C1: 38 topics (unchanged)
Total: 217 topics

Features:
- Complete coverage for everyday language mastery
- Practical topics: finance, insurance, property, automotive, pets, conversation management
- Detailed nuances for difficult grammar and conversational topics
- 95% fill-in-the-blank, 5% "Write:" translation questions
- Separate hint column (only when needed for disambiguation)
- Alternate correct responses for flexibility
- Granular topic coverage with bonus questions for difficult topics
- Spoken Italian preferences (elisions, contractions, etc.)
- Complete validation with rejection of any imperfect questions
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import pyperclip  # pip install pyperclip for clipboard support
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple, Set
from datetime import datetime

# --------------------------------
# Config
# --------------------------------
DEFAULT_BATCH_SIZE = 10  # Start with 10, can reduce to 5 for quality
TARGET_PER_LEVEL = 1500
OUTDIR = Path("data")
STATE_PATH = Path("state_progress_comprehensive.json")
PROMPT_DIR = Path("prompts")
LEVELS = ["A1", "A2", "B1", "B2", "C1"]

# Question type ratios
WRITE_RATIO = 0.05  # 5% translation questions
BONUS_RATIO = 0.20  # Up to 20% bonus for difficult topics
BONUS_MAX = 2  # Max 2 bonus items per batch

# --------------------------------
# Topics by Level (Complete comprehensive coverage)
# --------------------------------
TOPICS_BY_LEVEL: Dict[str, List[str]] = {
    "A1": [
        # ===== VERBS =====
        "Presente indicativo: verbi regolari -are",
        "Presente indicativo: verbi regolari -ere", 
        "Presente indicativo: verbi regolari -ire",
        "Presente indicativo: verbi regolari -ire con -isc- (tipo finire)",
        "Verbo essere (to be)",
        "Verbo avere (to have)",
        "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)",  # NEW
        "Verbi modali (uso base: potere, dovere, volere)",
        "Piacere (uso base: piace/piacciono)",
        
        # ===== GRAMMAR STRUCTURES =====
        "Forme negative semplici",
        "Parole interrogative",
        "C'è / Ci sono",
        "Congiunzioni base (e, ma, o, anche, perché)",  # NEW
        
        # ===== PREPOSITIONS & ARTICLES =====
        "Preposizioni semplici (uso base)",
        "Articoli determinativi e indeterminativi",
        
        # ===== NOUNS & ADJECTIVES =====
        "Nomi: genere e numero",
        "Aggettivi: accordo base",
        "Aggettivi possessivi (base)",
        "Dimostrativi (base)",
        
        # ===== PRONOUNS =====
        "Pronomi soggetto",
        
        # ===== ADVERBS & QUANTIFIERS =====
        "Molto/Poco (quantificatori e avverbi base)",  # NEW
        "Avverbi di modo base (bene, male, velocemente, lentamente)",  # NEW
        
        # ===== TIME =====
        "Giorni della settimana",
        "Mesi",
        "Stagioni",
        "Orario (telling time)",
        "Espressioni di tempo basilari",
        
        # ===== NUMBERS =====
        "Numeri 0–100 e prezzi",
        
        # ===== VOCABULARY TOPICS =====
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
        "Emozioni e stati d'animo (base)",  # NEW
        "Telefono e comunicazione (base)",  # NEW
        "Espressioni comuni",
    ],
    
    "A2": [
        # ===== FOUNDATION & REVIEW =====
        "Verbo Essere e Presente Indicativo",
        "Preposizioni Semplici",
        
        # ===== ARTICLES & PARTITIVES =====
        "Articoli partitivi",
        "Preposizioni articolate",
        "Confronto articoli (def/indef/partitivi)",
        
        # ===== REFLEXIVE VERBS =====
        "Verbi riflessivi (presente e passato prossimo)",
        
        # ===== PAST TENSES =====
        "Passato prossimo (reg/irr; essere/avere; accordo)",
        "Participio passato (comuni; irregolari frequenti)",
        "Imperfetto (introduzione)",
        "Imperfetto vs. Passato Prossimo",
        
        # ===== FUTURE & CONDITIONAL =====
        "Futuro semplice (base)",
        "Stare per + infinito",
        "Condizionale Presente",
        
        # ===== PROGRESSIVE =====
        "Stare + Gerundio",
        
        # ===== MODAL VERBS & SPECIAL VERBS =====
        "Verbi modali + infinito",
        "Piacere (con pronomi; passato)",
        "Volerci vs. Metterci",
        
        # ===== IMPERATIVES =====
        "Imperativo (tu/noi/voi; base)",
        "Imperativo Formale",
        
        # ===== PRONOUNS =====
        "Pronomi diretti",
        "Pronomi indiretti",
        "Pronomi Combinati",
        "Pronomi Relativi",
        "Aggettivi e Pronomi Indefiniti",
        "Particella ne (base)",
        "Particella ci (base)",
        
        # ===== IMPERSONAL =====
        "Si Impersonale",
        
        # ===== ADJECTIVES & ADVERBS =====
        "Avverbi di frequenza/tempo/luogo",
        "Comparativi e superlativi (base)",
        
        # ===== NEGATIVES =====
        "Forme negative composte (non...mai, non...niente, ecc.)",
        
        # ===== CONJUNCTIONS =====
        "Congiunzioni e connettivi (base)",
        
        # ===== TIME & NUMBERS =====
        "Espressioni di tempo (fa, da, tra/fra; ore)",
        "Numeri oltre 100",
        "Numeri ordinali",
        
        # ===== VOCABULARY TOPICS =====
        "Shopping",
        "Viaggi (trasporti, biglietti)",
        "Casa e quartiere",
        "Lavoro/ufficio (base)",
        "Scuola/Università (base)",
        "Salute (base)",
        "Ristorante (menu/prenotare/conti)",
        "Routine quotidiana dettagliata",
        "Tempo (weather) – esteso",
        "Ricordi (narrazione semplice)",
        "Programmi (piani futuri, inviti)",
        "Descrizioni fisiche e del carattere",
        "Prestare e chiedere in prestito",  # NEW
    ],
    
    "B1": [
        # ===== CORE B1 GRAMMAR =====
        "Imperfetto vs Passato prossimo",
        "Trapassato prossimo",
        "Futuro semplice (esteso)",
        "Condizionale presente",
        "Congiuntivo presente (introduzione: opinioni, emozioni base)",
        "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
        "Periodo ipotetico I tipo",
        
        # ===== PROGRESSIVE & GERUND =====
        "Stare + gerundio (progressivo)",
        "Gerundio (usi)",
        
        # ===== IMPERATIVES =====
        "Imperativo (pronomi atoni; negazione)",
        
        # ===== PRONOUNS =====
        "Pronomi combinati (glielo, me ne, ecc.)",
        "Ci e ne (avanzato)",
        "Pronomi relativi (che/cui; prep + cui)",
        "Pronomi indefiniti (comuni)",
        
        # ===== PASSIVE & IMPERSONAL =====
        "Passivo con essere (tempi principali)",
        "Si impersonale / si passivante (base)",
        
        # ===== REPORTED SPEECH =====
        "Discorso indiretto (base)",
        
        # ===== CONNECTORS & COMPARISONS =====
        "Connettivi di causa/effetto/concessione/ordine)",
        "Comparativi e superlativi (irregolarità)",
        
        # ===== MODAL VERBS =====
        "Verbi modali + pronomi clitici",
        
        # ===== B1 GAP-FILLING TOPICS =====
        "Sapere vs. Conoscere",
        "Preposizioni + infinito (da, di, a)",
        "Suffissi (diminutivi, accrescitivi, peggiorativi)",
        "Pronomi possessivi (vs aggettivi possessivi)",
        "Infinito passato (dopo aver/essere + participio)",
        
        # ===== VOCABULARY TOPICS =====
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
        "Animali domestici e veterinario",  # NEW
        "Proprietà e confini",  # NEW
        "Prestiti e restituzione (avanzato)",  # NEW
    ],
    
    "B2": [
        # ===== SUBJUNCTIVE & CONDITIONALS =====
        "Congiuntivo presente",
        "Congiuntivo passato",
        "Congiuntivo imperfetto",
        "Concordanza dei tempi (casi tipici)",
        "Periodo ipotetico II e III",
        "Condizionale passato",
        "Futuro anteriore",
        
        # ===== IMPLICIT FORMS =====
        "Forme implicite (infinito, gerundio, participio con valore temporale/causale)",
        
        # ===== PASSIVE & CAUSATIVE =====
        "Passivo con essere/venire; si passivante (avanzato)",
        "Verbi causativi (fare + infinito)",
        "Verbi causativi: lasciare (permettere)",  # NEW
        
        # ===== PRONOUNS =====
        "Pronomi relativi avanzati (il quale; cui articolate)",
        "Pronomi indefiniti/dimostrativi (avanzato)",
        "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)",  # NEW
        
        # ===== CONSTRUCTIONS =====
        "Costruzioni con gerundio/participio",
        
        # ===== PREPOSITIONS & REGISTER =====
        "Preposizioni complesse e locuzioni",
        "Registro e toni (formale/informale)",
        
        # ===== CONNECTORS =====
        "Connettivi complessi (benché, sebbene, purché, ecc.)",
        
        # ===== PRONOMINAL VERBS =====
        "Verbi pronominali (andarsene, cavarsela, farcela, ecc.)",
        
        # ===== REPORTED SPEECH =====
        "Discorso indiretto (avanzato)",
        
        # ===== PRACTICAL LIFE TOPICS (NEW) =====
        "Finanza personale (banca, prestiti, investimenti, pensione)",  # NEW
        "Assicurazioni (salute, auto, casa, vita)",  # NEW
        "Immobili (comprare, affittare, mutui, contratti)",  # NEW
        "Automobile (manutenzione, riparazioni, assicurazioni, guasti)",  # NEW
        "Salute avanzata (procedure mediche, malattie croniche, specialisti, salute mentale)",  # NEW
        "Cucina e gastronomia (ricette, tecniche, ingredienti, vino)",  # NEW
        "Questioni legali personali (contratti, diritti consumatori, cause)",  # NEW
        "Casa e giardinaggio (fai-da-te, piante, ristrutturazioni, elettrodomestici)",  # NEW
        "Fitness e benessere (palestra, allenamento, nutrizione)",  # NEW
        "Animali domestici - cure avanzate e problemi",  # NEW
        
        # ===== CONVERSATIONAL MANAGEMENT (NEW) =====
        "Gestione delle conversazioni difficili",  # NEW
        "Conflitto e risoluzione (avanzato)",  # NEW
        
        # ===== EXISTING VOCABULARY =====
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
    ],
}

# --------------------------------
# Topic-specific nuances for difficult topics
# --------------------------------
TOPIC_NUANCES = {
    # ==================== A1 NUANCES ====================
    
    "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)": """
    Essential high-frequency irregular verbs - must be mastered at A1:
    
    ANDARE (to go):
    • Present: vado, vai, va, andiamo, andate, vanno
    • "Vado al bar" (I go to the bar)
    • "Dove vai?" (Where are you going?)
    
    FARE (to do/make):
    • Present: faccio, fai, fa, facciamo, fate, fanno
    • "Faccio colazione" (I have breakfast)
    • "Cosa fai?" (What are you doing?)
    
    VENIRE (to come):
    • Present: vengo, vieni, viene, veniamo, venite, vengono
    • "Vengo domani" (I'm coming tomorrow)
    • "Vieni con me?" (Are you coming with me?)
    
    STARE (to stay/be):
    • Present: sto, stai, sta, stiamo, state, stanno
    • "Come stai?" (How are you?)
    • "Sto bene" (I'm well)
    
    DARE (to give):
    • Present: do, dai, dà, diamo, date, danno
    • "Ti do un libro" (I give you a book)
    
    USCIRE (to go out):
    • Present: esco, esci, esce, usciamo, uscite, escono
    • "Esco stasera" (I'm going out tonight)
    
    These verbs are used constantly in everyday Italian and cannot be avoided at A1 level.
    """,
    
    # ==================== A2 NUANCES ====================
    
    "Imperfetto vs. Passato Prossimo": """
    MUST cover these contrasts:
    - Completed Action vs. Ongoing Action
    - Specific Event vs. Habitual Action  
    - Background Description vs. Main Event
    - Modal Verbs (usage with imperfetto vs. passato prossimo)
    - Time expressions: sempre, spesso, di solito (→ imperfetto) vs. ieri, lunedì scorso (→ passato prossimo)
    Examples: 
      • "Mentre mangiavo, è arrivato" (background vs. event)
      • "Da bambino andavo sempre al mare" (habitual) vs. "Ieri sono andato al mare" (specific)
      • "Volevo uscire" (ongoing desire) vs. "Ho voluto uscire" (decided to go out)
    """,
    
    "Passato prossimo (reg/irr; essere/avere; accordo)": """
    Focus on:
    - Auxiliary selection (essere vs. avere)
    - Agreement with essere (Maria è andata, Loro sono arrivati)
    - Common irregular past participles (fatto, stato, detto, visto, scritto)
    - Motion verbs → essere (andare, venire, partire, arrivare)
    - Reflexive verbs → essere (mi sono svegliato/a)
    - No agreement with avere (unless direct object pronoun precedes)
    """,
    
    "Pronomi Combinati": """
    Focus on combined pronouns:
    - me lo/la/li/le, te lo/la/li/le
    - glielo/gliela/glieli/gliele (for lui, lei, loro, Lei)
    - ce lo/la/li/le, ve lo/la/li/le
    - Word order and attachment to infinitives
    - Changes with imperatives (Dammi → Dammelo!)
    Examples:
      • "Te lo do" (I give it to you)
      • "Glielo mando" (I send it to him/her)
      • "Puoi darmelo?" (Can you give it to me?)
    """,
    
    "Pronomi Relativi": """
    Cover relative pronouns:
    - che (that/which/who) - most common, subject or direct object
    - cui (which/whom) - used after prepositions (di cui, a cui, con cui)
    - il quale/la quale/i quali/le quali - formal alternative
    - chi (those who/whoever) - as subject
    Examples:
      • "Il libro che leggo" (that I read - direct object)
      • "L'uomo che parla" (who speaks - subject)
      • "La persona di cui parlo" (about whom I speak)
      • "Chi studia, impara" (whoever studies, learns)
    """,
    
    "Si Impersonale": """
    Cover impersonal constructions:
    - Si + third person verb for "one/we/people" in general
    - Si mangia bene qui (One eats well here)
    - Si dice che... (It is said that... / People say that...)
    - Agreement with plural nouns: Si vendono case (Houses are sold)
    - Difference from passive: active meaning, no agent
    Examples:
      • "In Italia si beve molto caffè" (In Italy people drink a lot of coffee)
      • "Come si dice in italiano?" (How do you say it in Italian?)
    """,
    
    "Imperativo Formale": """
    Focus on formal imperative (Lei form):
    - Uses congiuntivo presente for Lei: Venga! Faccia! Sia!
    - Negative: Non venga! Non faccia!
    - With pronouns: Me lo dia! (Give it to me!)
    - Common irregular forms: vada, venga, faccia, dica, abbia, sia
    - Always formal register for commands to strangers, elders, customers
    """,
    
    "Volerci vs. Metterci": """
    Distinguish between:
    - Volerci (impersonal - time/things needed, always 3rd person)
      • Ci vuole un'ora (It takes an hour)
      • Ci vogliono due ore (It takes two hours)
    - Metterci (personal - time someone/something takes)
      • Ci metto un'ora (I take an hour)
      • Ci mettiamo due ore (We take two hours)
    Key: volerci = "be necessary", metterci = "take (time)" with agent
    """,
    
    "Stare + Gerundio": """
    Present progressive construction:
    - Formation: stare (present) + gerund (-ando/-endo)
    - Sto mangiando (I am eating right now)
    - Regular gerunds: parlare → parlando, leggere → leggendo, dormire → dormendo
    - Irregular gerunds: fare → facendo, dire → dicendo, bere → bevendo
    - Use for actions happening RIGHT NOW (not habitual)
    - Pronouns can attach: Sto mangiandolo OR Lo sto mangiando
    """,
    
    "Condizionale Presente": """
    Cover:
    - Formation: future stem + conditional endings (-ei, -esti, -ebbe, -emmo, -este, -ebbero)
    - Polite requests: Vorrei un caffè, Potrebbe aiutarmi?
    - Hypothetical situations: Sarebbe bello, Andrei volentieri
    - Advice and suggestions: Dovresti studiare di più
    - Irregular stems same as future: essere → sarei, avere → avrei, fare → farei
    """,
    
    "Forme negative composte (non...mai, non...niente, ecc.)": """
    Double negative constructions (standard in Italian):
    - Non... mai (never): Non vado mai al cinema
    - Non... niente/nulla (nothing): Non ho niente da dire
    - Non... nessuno (nobody): Non vedo nessuno
    - Non... più (not anymore/no longer): Non fumo più
    - Non... ancora (not yet): Non è ancora arrivato
    - Can start with negative word: Mai vado al cinema (emphatic)
    Note: Unlike English, Italian REQUIRES double negatives
    """,
    
    "Congiunzioni e connettivi (base)": """
    Basic conjunctions and connectors:
    - Coordination: e (and), o (or), ma (but), però (however)
    - Cause: perché (because), siccome (since)
    - Effect: quindi (therefore), allora (so/then), perciò (therefore)
    - Addition: anche (also), inoltre (moreover)
    - Contrast: ma (but), però (however/but), invece (instead)
    - Time: quando (when), mentre (while), prima di (before), dopo (after)
    Examples:
      • "Sono stanco, quindi vado a casa"
      • "Mi piace l'Italia ma è cara"
      • "Studio perché voglio imparare"
    """,
    
    "Numeri ordinali": """
    Ordinal numbers for dates, rankings, floors:
    - Formation: primo, secondo, terzo, quarto, quinto, sesto, settimo, ottavo, nono, decimo
    - From 11th: add -esimo to cardinal (undici → undicesimo, venti → ventesimo)
    - Agreement: il primo piano, la prima volta, i primi giorni
    - Abbreviations: 1º (primo), 2º (secondo), 3ª (terza)
    - Common uses:
      • Dates: il primo maggio (May 1st) - note: only "primo" for 1st, rest use cardinals
      • Floors: il terzo piano (3rd floor)
      • Centuries: il ventesimo secolo (20th century)
      • Rankings: il secondo posto (2nd place)
    """,
    
    "Preposizioni articolate": """
    Contracted prepositions with articles (mandatory, not optional):
    - di + il/lo/la/i/gli/le → del/dello/della/dei/degli/delle
    - a + il/lo/la/i/gli/le → al/allo/alla/ai/agli/alle
    - da + il/lo/la/i/gli/le → dal/dallo/dalla/dai/dagli/dalle
    - in + il/lo/la/i/gli/le → nel/nello/nella/nei/negli/nelle
    - su + il/lo/la/i/gli/le → sul/sullo/sulla/sui/sugli/sulle
    Note: NEVER say "di il" - always use contraction
    Examples: Vado al cinema, Vengo dalla stazione, Il libro del professore
    """,
    
    "Pronomi diretti": """
    Direct object pronouns (replace direct objects):
    - mi (me), ti (you-informal), lo/la (him/her/it), ci (us), vi (you-plural), li/le (them)
    - La (you-formal) - capitalized for respect
    - Placement: BEFORE conjugated verb: "Lo vedo" (I see him/it)
    - Attached to infinitive: "Voglio vederlo" (I want to see him/it)
    - Agreement in passato prossimo: "L'ho vista" (I saw her - 'a' agrees with 'la')
    Common mistakes to avoid: using "lui/lei" as object (use lo/la instead)
    """,
    
    "Pronomi indiretti": """
    Indirect object pronouns (to/for whom):
    - mi (to me), ti (to you), gli/le (to him/to her), ci (to us), vi (to you-pl), gli/loro (to them)
    - Le (to you-formal)
    - Placement: same as direct (before verb or attached to infinitive)
    - Common with: dare, dire, telefonare, chiedere, rispondere, regalare, scrivere
    - No agreement in passato prossimo
    Examples:
      • "Gli telefono" (I call him)
      • "Le scrivo una lettera" (I write a letter to her)
      • "Mi piace" (It pleases me = I like it)
    Note: "loro" can go after verb: "Scrivo loro"
    """,
    
    "Prestare e chiedere in prestito": """
    Borrowing and lending - essential everyday interactions:
    
    PRESTARE (to lend - you give):
    • "Mi presti la macchina?" (Will you lend me the car?)
    • "Ti presto 10 euro" (I'll lend you 10 euros)
    • "Posso prestarti il libro" (I can lend you the book)
    
    PRENDERE IN PRESTITO (to borrow - you receive):
    • "Posso prendere in prestito il tuo telefono?" (Can I borrow your phone?)
    • "Ho preso in prestito dello zucchero" (I borrowed some sugar)
    
    RESTITUIRE (to return/give back):
    • "Ti restituisco il libro domani" (I'll return the book to you tomorrow)
    • "Quando me lo restituisci?" (When will you return it to me?)
    
    Common situations:
    • Household items: zucchero, sale, uova (sugar, salt, eggs)
    • Tools: trapano, martello, cacciavite (drill, hammer, screwdriver)
    • Money: "Mi presti 5 euro?" (Can you lend me 5 euros?)
    • Books, movies, clothes
    
    Polite forms:
    • "Potresti prestarmi...?" (Could you lend me...?)
    • "Ti dispiace se prendo in prestito...?" (Do you mind if I borrow...?)
    """,
    
    # ==================== B1 NUANCES ====================
    
    "Trapassato prossimo": """
    Past perfect (pluperfect) - action completed before another past action:
    - Formation: imperfetto of essere/avere + past participle
    - Aveva mangiato (he/she had eaten), Era andato/a (he/she had gone)
    - Time sequence: "Quando sono arrivato, Maria era già partita" (When I arrived, Maria had already left)
    - With "dopo che": "Dopo che aveva finito, è uscito" (After he had finished, he went out)
    - vs imperfetto: trapassato = completed before, imperfetto = ongoing in past
    - Agreement rules same as passato prossimo
    """,
    
    "Congiuntivo presente (introduzione: opinioni, emozioni base)": """
    Subjunctive present - expressing opinions, emotions, doubt:
    - After verbs of opinion: penso che, credo che, mi sembra che, dubito che
    - After expressions of emotion: sono felice che, mi dispiace che, ho paura che
    - After impersonal expressions: è importante che, bisogna che, è necessario che
    - When NOT to use it: so che + indicativo (certainty), è vero che + indicativo
    - Most common irregular forms: sia/siano (essere), abbia/abbiano (avere), faccia/facciano (fare), vada/vadano (andare)
    - Formation regular: -are → -i/-iamo/-iate/-ino, -ere/-ire → -a/-iamo/-iate/-ano
    Examples:
      • "Penso che sia importante" (I think it's important)
      • "Sono felice che tu venga" (I'm happy you're coming)
      • "Dubito che lo sappia" (I doubt he knows it)
    """,
    
    "Accordo del participio passato (con pronomi diretti, ne, riflessivi)": """
    Past participle agreement - when and how:
    1. With essere: ALWAYS agrees with subject
       • "Maria è andata" (feminine), "Loro sono arrivati" (masculine plural)
    2. With direct object pronouns: agrees with pronoun
       • "L'ho vista" (I saw her - 'a' for feminine la)
       • "Li ho comprati" (I bought them - 'i' for masculine li)
    3. With "ne": agrees with what ne replaces
       • "Ne ho comprati tre" (I bought three of them - 'i' for masculine)
    4. With reflexive verbs: agrees with subject (uses essere)
       • "Maria si è svegliata" (Maria woke up - feminine)
       • "Ci siamo divertiti" (We had fun - masculine plural)
    5. When NOT to agree: avere + direct object (no pronoun)
       • "Ho visto Maria" (no agreement - Maria comes after)
    """,
    
    "Periodo ipotetico I tipo": """
    First conditional (real/likely conditions):
    - Structure: Se + presente indicativo, futuro/imperativo/presente
    - "Se piove, resterò a casa" (If it rains, I'll stay home)
    - "Se hai tempo, chiamami" (If you have time, call me)
    - "Se studio, imparo" (If I study, I learn - general truth)
    - For real, likely, or possible conditions (not hypothetical)
    - Can also use future in both clauses: "Se pioverà, resterò a casa"
    Note: Type II (improbable) uses imperfect subjunctive + conditional
    """,
    
    "Si impersonale / si passivante (base)": """
    Two different constructions with "si":
    
    1. Si impersonale (impersonal "one/people"):
       • Si + 3rd person singular verb (usually)
       • "In Italia si mangia bene" (In Italy one eats well / people eat well)
       • "Si dice che..." (People say that... / It is said that...)
       • Active meaning, focus on action not agent
    
    2. Si passivante (passive with si):
       • Si + 3rd person verb (agrees with subject)
       • "Si vende casa" (House for sale - singular)
       • "Si vendono case" (Houses for sale - plural)
       • Passive meaning, focus on what is done
    
    Difference: impersonale = active focus on people doing, passivante = passive focus on things being done
    """,
    
    "Discorso indiretto (base)": """
    Reported speech (indirect discourse) - basic transformations:
    
    1. Tense changes (when reporting verb is past):
       • Presente → Imperfetto: "Sono stanco" → Ha detto che era stanco
       • Passato prossimo → Trapassato prossimo: "Ho mangiato" → Ha detto che aveva mangiato
       • Futuro → Condizionale passato: "Verrò" → Ha detto che sarebbe venuto
    
    2. Time expression changes:
       • oggi → quel giorno, domani → il giorno dopo, ieri → il giorno prima
       • ora → allora, fa → prima
    
    3. Pronoun changes:
       • Direct: "Io sono stanco" → Marco ha detto che lui era stanco
       • Possessive: "il mio libro" → "il suo libro"
    
    Examples:
      • Direct: Maria dice: "Vado al cinema"
      • Indirect: Maria dice che va al cinema (present reporting → no change)
      • Indirect: Maria ha detto che andava al cinema (past reporting → imperfetto)
    """,
    
    "Ci e ne (avanzato)": """
    Advanced uses beyond A2 basics:
    
    CI:
    1. Place (there): "Ci vado domani" (I'm going there tomorrow)
    2. About it/to it: "Ci penso" (I'm thinking about it), "Ci credo" (I believe in it)
    3. With verbs: volerci, metterci, crederci, pensarci, contarci
    4. Replaces "a + something": "Penso a Maria" → "Ci penso"
    
    NE:
    1. Quantity: "Ne voglio tre" (I want three of them)
    2. About it/of it: "Ne parliamo?" (Shall we talk about it?)
    3. Replaces "di + something": "Parlo di Maria" → "Ne parlo"
    4. Partitive: "Vuoi del pane?" → "Sì, ne voglio"
    
    Combined with other pronouns:
    • me ne, te ne, se ne, ce ne, ve ne
    • "Me ne vado" (I'm leaving), "Te ne do tre" (I'll give you three)
    """,
    
    "Passivo con essere (tempi principali)": """
    Passive voice with essere - focus on what is done, not who does it:
    
    Formation: essere (any tense) + past participle (agrees with subject)
    
    Main tenses:
    • Presente: "La casa è costruita" (The house is built)
    • Imperfetto: "La casa era costruita" (The house was being built)
    • Passato prossimo: "La casa è stata costruita" (The house was built/has been built)
    • Futuro: "La casa sarà costruita" (The house will be built)
    • Condizionale: "La casa sarebbe costruita" (The house would be built)
    
    With agent (optional):
    • "Il libro è scritto da Dante" (The book is written by Dante)
    • Use "da" to introduce the agent
    
    When to use passive:
    • Focus on the action/result rather than who does it
    • Agent unknown or unimportant
    • Formal/written language (more common than in English)
    
    Alternative: si passivante (Si vendono case = Houses are sold)
    """,
    
    "Comparativi e superlativi (irregolarità)": """
    Irregular comparatives and superlatives:
    
    Irregular forms:
    • buono → migliore (better), il migliore / ottimo (the best)
    • cattivo → peggiore (worse), il peggiore / pessimo (the worst)
    • grande → maggiore (bigger/greater/older), il maggiore / massimo (the biggest/greatest)
    • piccolo → minore (smaller/younger), il minore / minimo (the smallest)
    
    When to use:
    • "più buono" vs "migliore": both correct, migliore more common for quality
    • "più cattivo" vs "peggiore": similar, peggiore for quality
    • "più grande" vs "maggiore": maggiore often for age (fratello maggiore = older brother)
    
    Comparisons with "di" vs "che":
    • "di + article": "Maria è più alta di Luca" (comparing two people/things)
    • "che": "È più facile che difficile" (comparing two qualities)
    • "che": "Lavoro più a Roma che a Milano" (comparing two prepositions)
    
    Superlatives:
    • Relative: "il più alto" (the tallest), "la più bella" (the most beautiful)
    • Absolute: "altissimo" (very tall), "bellissima" (very beautiful)
    • With irregular: "il migliore di tutti" (the best of all)
    """,
    
    "Gerundio (usi)": """
    Gerund uses beyond progressive (stare + gerundio):
    
    1. Cause (because): "Essendo stanco, sono andato a letto" (Being tired, I went to bed)
    2. Time (while): "Camminando, ho incontrato Maria" (While walking, I met Maria)
    3. Condition (if/by): "Studiando, imparerai" (By studying, you'll learn)
    4. Manner (how): "È uscito correndo" (He left running)
    
    Formation:
    • Regular: -are → -ando, -ere → -endo, -ire → -endo
    • Irregular: fare → facendo, dire → dicendo, bere → bevendo
    
    With pronouns:
    • Attached to end: "mangiandolo" (eating it), "vedendola" (seeing her)
    • Or before stare: "Lo sto mangiando" OR "Sto mangiandolo"
    
    Gerund vs infinitive:
    • After prepositions (except per): use infinitive
    • "Prima di mangiare" (before eating - infinitive)
    • "Pur essendo..." (while being - gerund after pur)
    """,
    
    "Pronomi combinati (glielo, me ne, ecc.)": """
    Combined pronouns - when direct and indirect pronouns come together:
    
    Formation rules:
    1. Indirect changes: mi→me, ti→te, ci→ce, vi→ve
    2. gli/le/Le → glie- (combines with direct: glielo, gliela, glieli, gliele)
    3. Order: INDIRECT + DIRECT
    
    Examples:
    • Mi + lo → me lo: "Me lo dai?" (Will you give it to me?)
    • Ti + la → te la: "Te la mando" (I'll send it to you)
    • Gli/Le + lo → glielo: "Glielo dico" (I tell it to him/her/you-formal)
    • Ci + ne → ce ne: "Ce ne sono tre" (There are three of them)
    
    With infinitives:
    • Attached: "Puoi darmelo?" (Can you give it to me?)
    • Or before modal: "Me lo puoi dare?"
    
    With imperatives:
    • Positive: attached: "Dammelo!" (Give it to me!)
    • Negative informal: separate or attached: "Non darmelo!" or "Non me lo dare!"
    • Negative formal: separate: "Non me lo dia!"
    
    Common mistakes:
    • *"mi lo" (wrong) → "me lo" (correct)
    • Remember "glie-" stays together: "glielo" not "gli lo"
    """,
    
    "Sapere vs. Conoscere": """
    Two verbs for "to know" - different uses:
    
    SAPERE (to know facts, information, how to do something):
    • Facts: "So che è vero" (I know it's true)
    • Information: "Sai dove abita?" (Do you know where he lives?)
    • Skills: "So parlare italiano" (I know how to speak Italian / I can speak Italian)
    • With infinitive: ability/skill
    • Present: so, sai, sa, sappiamo, sapete, sanno
    
    CONOSCERE (to know people, places, be familiar with):
    • People: "Conosco Marco" (I know Marco / I'm acquainted with Marco)
    • Places: "Conosco Roma" (I know Rome / I'm familiar with Rome)
    • Works: "Conosci questo libro?" (Do you know this book? / Are you familiar with it?)
    • Never with infinitive
    • Present: conosco, conosci, conosce, conosciamo, conoscete, conoscono
    
    Key difference:
    • Sapere = know information (followed by che, quando, dove, come, perché, infinitive)
    • Conoscere = be familiar/acquainted (followed by noun - person, place, thing)
    
    Common errors:
    • *"Conosco che è vero" (wrong) → "So che è vero" (correct)
    • *"So Marco" (wrong) → "Conosco Marco" (correct)
    • *"Conosco parlare" (wrong) → "So parlare" (correct)
    """,
    
    "Preposizioni + infinito (da, di, a)": """
    Prepositions with infinitives - mandatory patterns:
    
    DA + infinitive:
    • Something to be done: "Ho molto da fare" (I have a lot to do)
    • Purpose: "macchina da scrivere" (typewriter - machine for writing)
    • Characteristic: "È facile da capire" (It's easy to understand)
    • After qualcosa/niente: "qualcosa da bere" (something to drink)
    
    DI + infinitive:
    • After many verbs: cercare di, decidere di, finire di, smettere di, avere bisogno di
    • "Cerco di studiare" (I try to study)
    • "Ho bisogno di dormire" (I need to sleep)
    • After adjectives + essere: "Sono felice di vederti" (I'm happy to see you)
    
    A + infinitive:
    • After motion verbs: andare a, venire a
    • After verbs of beginning: cominciare a, iniziare a, mettersi a
    • "Vado a mangiare" (I'm going to eat)
    • "Comincio a capire" (I'm beginning to understand)
    • Tendency: "È lento a capire" (He's slow to understand)
    
    No preposition:
    • Modal verbs: dovere, potere, volere, sapere
    • "Devo studiare" (I must study) - no preposition!
    
    Common verb patterns to memorize:
    • cercare di, decidere di, finire di, smettere di, dimenticare di, promettere di
    • cominciare a, continuare a, imparare a, riuscire a, insegnare a
    • avere da (fare), essere da (fare)
    """,
    
    "Suffissi (diminutivi, accrescitivi, peggiorativi)": """
    Suffixes that modify meaning - very common in Italian:
    
    DIMINUTIVES (small, cute, affectionate):
    • -ino/-ina: gattino (kitten), casina (little house), piccolino (very small)
    • -etto/-etta: libretto (booklet), ragazzetto (little boy)
    • -ello/-ella: asinello (little donkey), stradella (little street)
    • -uccio/-uccia: calduccio (nice and warm), casuccia (cute little house)
    
    AUGMENTATIVES (big, impressive):
    • -one/-ona: librone (big book), nasone (big nose), ragazzone (big guy)
    • Can change gender: la porta → il portone (big door/gate - becomes masculine)
    
    PEJORATIVES (bad, ugly, contemptuous):
    • -accio/-accia: tempaccio (awful weather), ragazzaccio (bad boy), parolaccia (swear word)
    • -astro/-astra: poetastro (bad poet), figliastro (stepson)
    • -ucolo/-ucola: medicocucolo (quack doctor)
    
    Multiple suffixes:
    • Can combine: casa → casetta → casettina (house → little house → very little house)
    
    Important notes:
    • Not all words accept all suffixes
    • Some forms are lexicalized: mattone (brick, not "big morning")
    • Context matters: "che bellino!" (how cute!) vs "È piccolino" (it's rather small)
    • Very productive in spoken Italian - shows familiarity/informality
    """,
    
    "Pronomi possessivi (vs aggettivi possessivi)": """
    Possessive pronouns (standing alone) vs possessive adjectives (with noun):
    
    POSSESSIVE ADJECTIVES (with noun):
    • il mio libro (my book), la mia casa (my house)
    • Always with article (except: mio padre, mia madre, mio fratello, mia sorella when singular)
    • Agreement: with the thing possessed, not the possessor
    
    POSSESSIVE PRONOUNS (standalone - replace noun):
    • il mio (mine), la mia (mine-fem), i miei (mine-pl-masc), le mie (mine-pl-fem)
    • Must keep the article
    • Agreement: with the thing being replaced
    
    Examples:
    • "Questo è il mio libro" (adjective - This is my book)
    • "Questo libro è il mio" (pronoun - This book is mine)
    • "La mia macchina è rossa, la tua è blu" (Mine is red, yours is blue)
    
    Special cases:
    • With essere: article optional
      "È mio" or "È il mio" (It's mine) - both correct
      "Sono miei" or "Sono i miei" (They're mine)
    
    All forms:
    • il mio/la mia/i miei/le mie (mine)
    • il tuo/la tua/i tuoi/le tue (yours-informal)
    • il suo/la sua/i suoi/le sue (his/hers/its)
    • il nostro/la nostra/i nostri/le nostre (ours)
    • il vostro/la vostra/i vostri/le vostre (yours-plural)
    • il loro/la loro/i loro/le loro (theirs - no agreement!)
    
    Common errors:
    • *"È il mio" when you mean the adjective (should be "il mio libro")
    • Forgetting article with pronoun: *"È mio libro" (wrong) → "È il mio libro" or "È mio" (correct)
    """,
    
    "Infinito passato (dopo aver/essere + participio)": """
    Past infinitive - expressing an action that happened before another:
    
    Formation:
    • avere/essere (infinitive) + past participle
    • After AVERE: dopo aver mangiato (after eating/having eaten)
    • After ESSERE: dopo essere andato/a/i/e (after going/having gone)
    
    When to use:
    1. After "dopo": "Dopo aver mangiato, sono uscito" (After eating, I went out)
    2. With verbs of regret: "Mi dispiace di essere arrivato tardi" (I'm sorry for arriving late)
    3. With verbs of belief: "Credo di aver capito" (I think I understood)
    4. After senza: "È uscito senza aver chiuso la porta" (He left without having closed the door)
    
    Agreement with essere:
    • Must agree with subject: "Dopo essere arrivata, Maria ha chiamato" (After arriving, Maria called)
    • Reflexive: "Dopo essermi svegliato, ho fatto colazione" (After waking up, I had breakfast)
    
    vs simple infinitive:
    • Simple: simultaneous or general: "Prima di mangiare" (before eating - not yet done)
    • Past: completed before: "Dopo aver mangiato" (after having eaten - already done)
    
    Common expressions:
    • Dopo aver fatto (after doing)
    • Senza aver detto (without saying)
    • Prima di essere andato (before having gone - rare, usually use simple)
    • Credo di aver dimenticato (I think I forgot)
    
    Important:
    • Subject of main verb must be same as implied subject of infinitive
    • "Dopo aver mangiato, sono uscito" (I ate, I went out - same subject ✓)
    • Can't say: *"Dopo aver mangiato, il film è iniziato" (I ate, the film started - different subjects ✗)
    """,
    
    "Animali domestici e veterinario": """
    Pet care and veterinary topics - practical everyday vocabulary:
    
    COMMON PETS:
    • il cane (dog), il gatto (cat), il pesce (fish), l'uccellino (bird)
    • il coniglio (rabbit), il criceto (hamster)
    
    BASIC CARE:
    • il cibo/il mangime (food), l'acqua (water)
    • la passeggiata (walk): "Devo portare il cane a passeggio"
    • la toelettatura (grooming)
    • la lettiera (litter box - for cats)
    • la cuccia (dog bed/house)
    
    VETERINARY:
    • il veterinario (vet), la clinica veterinaria (vet clinic)
    • le vaccinazioni (vaccinations): "Il gatto ha bisogno di vaccini"
    • la rabbia (rabies), l'antirabbica (rabies vaccine)
    • il trattamento antipulci (anti-flea treatment)
    • il vermifugo (dewormer)
    • la visita (checkup): "Devo portare il cane dal veterinario per una visita"
    
    HEALTH ISSUES:
    • malato/a (sick), ferito/a (injured)
    • le pulci (fleas), le zecche (ticks)
    • la diarrea (diarrhea), il vomito (vomiting)
    • l'allergia (allergy)
    
    Common phrases:
    • "Il gatto non sta bene" (The cat isn't well)
    • "Ha bisogno di un richiamo" (He/she needs a booster shot)
    • "Quanto costa la visita?" (How much does the visit cost?)
    """,
    
    "Proprietà e confini": """
    Property boundaries and asserting rights - practical commands:
    
    PROPERTY VOCABULARY:
    • la proprietà privata (private property)
    • il terreno (land), il confine (boundary)
    • il proprietario (owner), l'inquilino (tenant)
    • il cartello (sign): "Proprietà privata - Vietato l'ingresso"
    
    ASSERTIVE COMMANDS (telling someone to leave):
    • "Questa è proprietà privata" (This is private property)
    • "Deve andarsene" (You must leave - formal)
    • "Devi andartene" (You must leave - informal)
    • "Non può entrare qui" (You can't enter here)
    • "Fuori dalla mia proprietà!" (Off my property!)
    • "Se ne vada!" (Go away! - formal)
    • "Vattene!" (Go away! - informal)
    
    LEGAL/BOUNDARY LANGUAGE:
    • vietato (forbidden): "Vietato l'accesso" (No entry)
    • l'intrusione (trespassing)
    • il permesso (permission): "Non ha il permesso di entrare"
    • chiamare la polizia (call the police)
    
    ESCALATION:
    • "Le chiedo gentilmente di andarsene" (I'm asking you politely to leave)
    • "Se non se ne va, chiamo la polizia" (If you don't leave, I'll call the police)
    • "Sto chiamando la polizia" (I'm calling the police)
    
    Important note: These commands use formal Lei or informal tu depending on context
    """,
    
    # ==================== B2 NUANCES ====================
    
    "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)": """
    Emphatic/stressed pronouns - for emphasis and reflexivity:
    
    FORMS:
    • me stesso/a (myself), te stesso/a (yourself-informal)
    • lui stesso, lei stessa (himself, herself)
    • sé stesso/a (himself/herself/itself - reflexive)
    • noi stessi/e (ourselves), voi stessi/e (yourselves)
    • loro stessi/e (themselves)
    • Lei stesso/a (yourself-formal)
    
    USES:
    
    1. Emphasis (I did it MYSELF):
    • "L'ho fatto io stesso" (I did it myself)
    • "Lo ha detto lui stesso" (He said it himself)
    • "Devi vederlo tu stesso" (You have to see it yourself)
    
    2. Reflexive emphasis (thinking only of oneself):
    • "Pensa solo a se stesso" (He only thinks about himself)
    • "Parla sempre di sé stessa" (She always talks about herself)
    • "Non pensare solo a te stesso" (Don't only think about yourself)
    
    3. By oneself/alone:
    • "L'ho fatto da me stesso" or "L'ho fatto da solo" (I did it by myself/alone)
    • Note: "da solo" (alone) vs "da me stesso" (by myself - emphasis on not needing help)
    
    4. Contrast/correction:
    • "Non lui, ma io stesso" (Not him, but me myself)
    
    SÉ vs SE:
    • "sé" (accented) = stressed form: "pensa a sé" (thinks about himself)
    • "se" (unaccented) = reflexive pronoun: "si lava" (he washes himself)
    • Exception: "se stesso" (no accent because "stesso" makes it clear)
    
    Common expressions:
    • "di per sé" (in itself)
    • "da sé" (by itself, automatically)
    • "essere padrone di se stesso" (to be master of oneself)
    • "fare da sé" (to do it oneself)
    """,
    
    "Verbi causativi: lasciare (permettere)": """
    Causative with "lasciare" - letting/allowing someone to do something:
    
    LASCIARE vs FARE (different causatives):
    • LASCIARE = let/allow (person chooses to do it)
    • FARE = make/have (person is made/caused to do it)
    
    LASCIARE + infinitive:
    • "Lasciami parlare" (Let me speak)
    • "Non lasciarmi solo" (Don't leave me alone)
    • "Lascia perdere" (Let it go / Forget about it)
    • "Lascio che tu decida" (I'll let you decide)
    
    LASCIARE CHE + subjunctive:
    • "Lascio che vadano" (I let them go)
    • "Non lascio che tu esca" (I don't let you go out)
    • "Lascia che ti aiuti" (Let me help you)
    
    vs PERMETTERE:
    • PERMETTERE = permit/allow (more formal)
    • "Mi permette di entrare?" (May I enter? - formal)
    • "Non ti permetto di parlare così" (I don't allow you to speak like that)
    • "Se il tempo lo permette" (Weather permitting)
    
    vs FARE:
    • "Faccio riparare la macchina" (I have the car repaired - someone else does it)
    • "Lascio che lui ripari la macchina" (I let him repair the car - he chooses to)
    
    Common expressions with LASCIARE:
    • "Lasciami in pace" (Leave me alone)
    • "Lascia stare" (Leave it alone / Don't bother)
    • "Lascia perdere" (Forget it / Never mind)
    • "Lasciarsi andare" (to let oneself go)
    • "Lasciar correre" (to let it slide)
    
    With pronouns:
    • "Lascialo fare" (Let him do it)
    • "Non lasciarla andare" (Don't let her go)
    """,
    
    "Gestione delle conversazioni difficili": """
    Managing difficult conversations - essential communication skills:
    
    1. NAMING THE PATTERN (making invisible patterns visible):
    • "Noto che continuiamo a tornare su X quando provo a discutere Y"
      (I notice we keep coming back to X when I try to discuss Y)
    • "Questa è la terza volta che il discorso si sposta quando porto su [argomento]"
      (This is the third time the topic has shifted when I bring up [issue])
    • "Sento una certa resistenza a questo argomento particolare"
      (I'm sensing some resistance to this particular subject)
    
    2. ANCHORING & REDIRECTING:
    • "Lasciami finire questo pensiero, poi mi piacerebbe sentire quello"
      (Let me finish this thought first, then I'd love to hear about that)
    • "Voglio tornare a quello che stavo dicendo su..."
      (I want to come back to what I was saying about...)
    • "Tieni quel pensiero - devo completare questo punto prima"
      (Hold that thought - I need to complete this point first)
    • "Prima di andare avanti, ho bisogno di una risposta a..."
      (Before we move on, I need an answer to...)
    
    3. SPOTTING DEFLECTION/MANIPULATION:
    • "Questa è una questione separata - affrontiamo prima questa"
      (That's a separate issue - let's address this one first)
    • "Non sono sicuro/a di come questo si colleghi a quello che stiamo discutendo"
      (I'm not sure how that relates to what we're discussing)
    • "Stai cambiando discorso. Possiamo rimanere su questo?"
      (You're changing the subject. Can we stay with this?)
    • "Quello sembra un argomento 'e allora' - concentriamoci sulla domanda attuale"
      (That sounds like a 'what about' argument - let's focus on the current question)
    
    4. REALITY-CHECKING:
    • "Lasciami assicurarmi di aver capito: stai dicendo che [ripetere la loro posizione]?"
      (Let me make sure I understand: are you saying [restate their position]?)
    • "Aiutami a conciliare queste due cose che hai detto..."
      (Help me reconcile these two things you've said...)
    • "Questo contraddice quello che hai detto prima - puoi chiarire?"
      (That contradicts what you mentioned earlier - can you clarify?)
    
    5. SETTING BOUNDARIES:
    • "Sono disposto/a a discutere questo, ma non in circolo"
      (I'm willing to discuss this, but not in circles)
    • "Abbiamo già affrontato questo. Cosa servirebbe per andare avanti?"
      (We've covered this already. What would it take to move forward?)
    • "Ho bisogno di una risposta diretta a questa domanda specifica"
      (I need a direct answer to this specific question)
    
    6. METACOGNITIVE CHECKS (discussing the discussion):
    • "Di cosa stiamo parlando veramente qui?"
      (What are we really talking about here?)
    • "Sento che stiamo discutendo a livello superficiale quando il vero problema è..."
      (It feels like we're discussing surface level when the real issue is...)
    • "Stiamo risolvendo un problema o solo sfogandoci?"
      (Are we solving a problem or just venting?)
    
    7. ADDRESSING LYING/DISHONESTY:
    • "Questo non corrisponde ai fatti che conosco"
      (This doesn't match the facts I know)
    • "Ho una versione diversa degli eventi"
      (I have a different version of events)
    • "Possiamo parlare di cosa è realmente successo?"
      (Can we talk about what actually happened?)
    
    8. QUESTIONS ANSWERED WITH QUESTIONS:
    • "Perché rispondi alle mie domande con un'altra domanda invece di rispondere?"
      (Why do you answer my questions with another question instead of actually answering?)
    • "Ho fatto una domanda specifica - posso avere una risposta prima?"
      (I asked a specific question - can I get an answer first?)
    """,
    
    "Conflitto e risoluzione (avanzato)": """
    Advanced conflict resolution - de-escalation and productive disagreement:
    
    1. ACKNOWLEDGING WITHOUT AGREEING:
    • "Capisco che tu la veda così" (I understand you see it that way)
    • "Sento che questo è importante per te" (I hear that this is important to you)
    • "Rispetto il tuo punto di vista, anche se non sono d'accordo"
      (I respect your point of view, even though I disagree)
    
    2. SEPARATING PROBLEM FROM PERSON:
    • "Non sono contro di te, sono contro questo comportamento"
      (I'm not against you, I'm against this behavior)
    • "Affrontiamo il problema, non l'un l'altro"
      (Let's address the problem, not each other)
    
    3. ASSERTIVE vs AGGRESSIVE:
    • Assertive: "Ho bisogno che tu..." (I need you to...)
    • Aggressive: "Devi..." (You must...)
    • Assertive: "Quando fai X, mi sento Y" (When you do X, I feel Y)
    • Aggressive: "Mi fai sempre sentire..." (You always make me feel...)
    
    4. DE-ESCALATION:
    • "Facciamo una pausa e ne riparliamo quando siamo più calmi"
      (Let's take a break and talk about this when we're calmer)
    • "Non voglio litigare. Voglio capire" (I don't want to fight. I want to understand)
    • "Possiamo ricominciare? Non volevo che andasse così"
      (Can we start over? I didn't want it to go this way)
    
    5. IDENTIFYING THE REAL ISSUE:
    • "Affrontiamo il vero problema" (Let's address the real problem)
    • "Non è davvero di questo che si tratta, vero?" (This isn't really what this is about, is it?)
    • "Qual è il problema di fondo?" (What's the underlying issue?)
    
    6. SOLUTION-FOCUSED:
    • "Stiamo risolvendo o solo lamentandoci?" (Are we solving or just complaining?)
    • "Cosa ci vorrebbe per risolvere questo?" (What would it take to solve this?)
    • "Possiamo trovare una soluzione che funzioni per entrambi?"
      (Can we find a solution that works for both of us?)
    
    7. TAKING RESPONSIBILITY:
    • "Mi dispiace. Avevo torto" (I'm sorry. I was wrong)
    • "La mia parte in questo è stata..." (My part in this was...)
    • "Avrei dovuto..." (I should have...)
    
    8. CONSTRUCTIVE CRITICISM:
    • "Ho un feedback da darti - va bene se ne parliamo?"
      (I have feedback for you - is it okay if we discuss it?)
    • "Posso suggerirti qualcosa?" (Can I suggest something to you?)
    • Using "I" statements: "Io vedo/penso/sento" not "Tu sei/fai sempre"
    """,
}

# Topic-specific resource URLs
TOPIC_RESOURCES = {
    "Passato prossimo": "https://www.lawlessitalian.com/grammar/passato-prossimo/",
    "Imperfetto": "https://www.lawlessitalian.com/grammar/imperfetto/",
    "Trapassato prossimo": "https://www.lawlessitalian.com/grammar/pluperfect/",
    "Congiuntivo presente": "https://www.lawlessitalian.com/grammar/subjunctive-present/",
    "Congiuntivo": "https://www.lawlessitalian.com/grammar/subjunctive/",
    "Articoli": "https://www.lawlessitalian.com/grammar/articles/",
    "Articoli partitivi": "https://www.lawlessitalian.com/grammar/partitive-articles/",
    "Preposizioni": "https://www.lawlessitalian.com/grammar/prepositions/",
    "Verbo essere": "https://www.lawlessitalian.com/grammar/essere-to-be/",
    "Verbo avere": "https://www.lawlessitalian.com/grammar/avere-to-have/",
    "Verbi modali": "https://www.lawlessitalian.com/grammar/modal-verbs/",
    "Verbi irregolari": "https://www.lawlessitalian.com/grammar/irregular-verbs/",
    "Pronomi": "https://www.lawlessitalian.com/grammar/pronouns/",
    "Imperativo": "https://www.lawlessitalian.com/grammar/imperative/",
    "Gerundio": "https://www.lawlessitalian.com/grammar/gerund/",
    "Infinito": "https://www.lawlessitalian.com/grammar/infinitive/",
    "Passivo": "https://www.lawlessitalian.com/grammar/passive-voice/",
    "Comparativi": "https://www.lawlessitalian.com/grammar/comparisons/",
    "Numeri": "https://www.lawlessitalian.com/vocabulary/numbers/",
    "Negatives": "https://www.lawlessitalian.com/grammar/negation/",
    "Sapere": "https://www.lawlessitalian.com/grammar/sapere-to-know/",
    "Conoscere": "https://www.lawlessitalian.com/grammar/conoscere-to-know/",
    "Suffixes": "https://www.lawlessitalian.com/grammar/suffixes/",
}

# Difficult topics that often need more practice
DIFFICULT_TOPICS = {
    # A1 difficult topics
    "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)",
    
    # A2 difficult topics
    "Passato prossimo (reg/irr; essere/avere; accordo)",
    "Imperfetto vs. Passato Prossimo",
    "Pronomi Combinati",
    "Pronomi Relativi",
    "Si Impersonale",
    "Volerci vs. Metterci",
    "Imperativo Formale",
    "Preposizioni articolate",
    "Forme negative composte (non...mai, non...niente, ecc.)",
    
    # B1 difficult topics
    "Imperfetto vs Passato prossimo",
    "Accordo del participio passato (con pronomi diretti, ne, riflessivi)",
    "Congiuntivo presente (introduzione: opinioni, emozioni base)",
    "Pronomi combinati (glielo, me ne, ecc.)",
    "Si impersonale / si passivante (base)",
    "Trapassato prossimo",
    "Discorso indiretto (base)",
    "Periodo ipotetico I tipo",
    "Ci e ne (avanzato)",
    "Passivo con essere (tempi principali)",
    "Sapere vs. Conoscere",
    "Preposizioni + infinito (da, di, a)",
    
    # B2 difficult topics
    "Concordanza dei tempi",
    "Periodo ipotetico II e III",
    "Congiuntivo passato",
    "Congiuntivo imperfetto",
    "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)",
    "Gestione delle conversazioni difficili",
}

# --------------------------------
# Schema
# --------------------------------
REQUIRED_FIELDS = [
    "complete_sentence", "question_text", "english_translation", "hint",
    "alternate_correct_responses", "option_a", "option_b", "option_c", "option_d",
    "correct_option", "cefr_level", "topic", "explanation", "resource",
]

@dataclass
class QA:
    complete_sentence: str
    question_text: str
    english_translation: str
    hint: str
    alternate_correct_responses: str
    option_a: str
    option_b: str
    option_c: str
    option_d: str
    correct_option: str
    cefr_level: str
    topic: str
    explanation: str
    resource: str

# --------------------------------
# State Management
# --------------------------------

def load_state() -> Dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"coverage": {}, "seen_questions": {}}

def save_state(state: Dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def load_existing_texts(level: str) -> Set[str]:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    path = OUTDIR / f"italian_{level}.csv"
    if not path.exists():
        return set()
    found = set()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "question_text" in row and row["question_text"]:
                found.add(row["question_text"].strip())
    return found

# --------------------------------
# Topic Planning
# --------------------------------

def plan_topics(level: str, state: Dict, batch_size: int) -> Tuple[List[str], Dict[str, int]]:
    """Plan topics with coverage stats and bonus questions for difficult topics."""
    cov = state["coverage"].setdefault(level, {})
    topics = TOPICS_BY_LEVEL[level]
    
    # Initialize coverage
    for t in topics:
        cov.setdefault(t, 0)
    
    # Calculate weights (inverse of coverage)
    weights = []
    for t in topics:
        base_weight = 1.0 / (1.0 + cov.get(t, 0))
        # Boost weight for difficult topics
        if t in DIFFICULT_TOPICS:
            base_weight *= 1.5
        weights.append(base_weight)
    
    # Select topics
    max_per_topic = max(2, batch_size // 3)
    counts = {t: 0 for t in topics}
    picks: List[str] = []
    
    for _ in range(batch_size):
        temp_topics = []
        temp_weights = []
        for t, w in zip(topics, weights):
            if counts[t] < max_per_topic:
                temp_topics.append(t)
                temp_weights.append(w)
        if not temp_topics:
            temp_topics = topics[:]
            temp_weights = weights[:]
        choice = random.choices(temp_topics, weights=temp_weights, k=1)[0]
        counts[choice] += 1
        picks.append(choice)
    
    # Calculate bonus questions for difficult topics
    difficult_count = sum(1 for t in picks if t in DIFFICULT_TOPICS)
    bonus_allowed = min(BONUS_MAX, int(difficult_count * 0.5))
    
    # Add bonus questions for difficult topics
    if bonus_allowed > 0:
        difficult_in_batch = [t for t in picks if t in DIFFICULT_TOPICS]
        for _ in range(bonus_allowed):
            if difficult_in_batch:
                picks.append(random.choice(difficult_in_batch))
    
    # Create plan counts
    plan_counts = {}
    for t in picks:
        plan_counts[t] = plan_counts.get(t, 0) + 1
    
    return picks, plan_counts

def get_coverage_stats(level: str, state: Dict) -> str:
    """Get formatted coverage statistics."""
    cov = state["coverage"].get(level, {})
    topics = TOPICS_BY_LEVEL[level]
    
    if not cov:
        return "No questions generated yet for this level."
    
    # Sort by coverage (least to most)
    sorted_topics = sorted(topics, key=lambda t: cov.get(t, 0))
    
    stats = []
    stats.append(f"Coverage for {level} ({len(topics)} topics):")
    stats.append(f"  Least covered topics:")
    for t in sorted_topics[:5]:
        count = cov.get(t, 0)
        marker = " [DIFFICULT]" if t in DIFFICULT_TOPICS else ""
        stats.append(f"    - {t}: {count} questions{marker}")
    
    total = sum(cov.values())
    stats.append(f"  Total questions: {total}")
    
    return "\n".join(stats)

# --------------------------------
# Sample Previous Questions
# --------------------------------

def sample_previous_questions(level: str, max_chars: int = 2000) -> List[str]:
    """Get a sample of previous questions to avoid duplicates."""
    seen = load_existing_texts(level)
    if not seen:
        return []
    
    items = list(seen)
    random.shuffle(items)
    out = []
    total = 0
    for q in items:
        q = q.strip()
        if not q:
            continue
        projected = total + len(q) + 3
        if projected > max_chars:
            break
        out.append(q)
        total = projected
    return out[:15]  # Max 15 examples

# --------------------------------
# Get Resource URL
# --------------------------------

def get_resource_url(topic: str) -> str:
    """Get the most specific resource URL for a topic."""
    # Check exact match
    if topic in TOPIC_RESOURCES:
        return TOPIC_RESOURCES[topic]
    
    # Check partial matches
    for key, url in TOPIC_RESOURCES.items():
        if key.lower() in topic.lower() or topic.lower() in key.lower():
            return url
    
    # Default to generic grammar page
    return "https://www.lawlessitalian.com/grammar/"

# --------------------------------
# Build Claude Prompt
# --------------------------------

def build_claude_prompt(level: str, topics_plan: List[str], batch_size: int, state: Dict) -> str:
    """Build optimized prompt for Claude with all requirements."""
    
    plan_counts: Dict[str, int] = {}
    for t in topics_plan:
        plan_counts[t] = plan_counts.get(t, 0) + 1
    
    prior_sample = sample_previous_questions(level)
    prior_block = "\n".join(f"- {q}" for q in prior_sample) if prior_sample else "(none available)"
    
    # Calculate question types
    total_questions = len(topics_plan)
    write_count = max(1, int(total_questions * WRITE_RATIO)) if total_questions >= 10 else 0
    fill_count = total_questions - write_count
    
    # Language preferences by level
    lang_prefs = ""
    if level == "A1":
        lang_prefs = """
For A1 level specifically:
- MINIMIZE dropped pronouns (use "Io vado" more than "Vado") to help learners
- MINIMIZE truncations (use "poco" not "po'", "grande" not "gran") for clarity
- Still use natural elisions (dell'acqua, c'è) and contractions where standard"""
    else:
        lang_prefs = """
For spoken Italian authenticity:
- PREFER elisions: dell'acqua (not della acqua), l'amico (not lo amico)
- PREFER contractions: c'è (not ci è), dell'università (not della università)
- PREFER dropped pronouns where natural: "Vado al mercato" (not always "Io vado")
- PREFER common truncations: un po' (not un poco), buon giorno (not buono giorno)"""

    # Build topic-specific instructions
    topic_instructions = []
    unique_topics = set(topics_plan)
    for topic in unique_topics:
        if topic in TOPIC_NUANCES:
            topic_instructions.append(f"\n### {topic}\n{TOPIC_NUANCES[topic]}")
    
    topic_notes_section = ""
    if topic_instructions:
        topic_notes_section = "\n## TOPIC-SPECIFIC REQUIREMENTS\n" + "\n".join(topic_instructions)

    prompt = f"""You are an expert Italian language educator creating questions for CEFR level {level}.

## CRITICAL REQUIREMENTS

Generate EXACTLY {total_questions} questions with perfect accuracy. Quality over quantity - every question must be 100% correct.

Question distribution:
- {fill_count} Fill-in-the-blank questions
- {write_count} "Write:" translation questions (if applicable)

## JSON STRUCTURE

```json
{{
  "questions": [
    {{
      "complete_sentence": "Grammatically perfect Italian sentence",
      "question_text": "Sentence with ONE ___ or 'Write: English sentence'",
      "english_translation": "Accurate English translation",
      "hint": "Only if needed for disambiguation (or empty string)",
      "alternate_correct_responses": "Semicolon-separated variants (or empty)",
      "option_a": "First option",
      "option_b": "Second option",
      "option_c": "Third option",
      "option_d": "Fourth option",
      "correct_option": "A, B, C, or D",
      "cefr_level": "{level}",
      "topic": "Exact topic from list",
      "explanation": "Clear grammar rule explanation",
      "resource": "Specific LawlessItalian.com URL when possible"
    }}
  ]
}}
```

## HINT PHILOSOPHY

Hints are ONLY for disambiguation when:
1. Multiple grammatically correct answers would change meaning/agreement
2. Similar words could be confused (e.g., tè vs tisana)
3. Context alone doesn't clarify gender/number/formality

Good hints:
- "feminine speaker" (for agreement)
- "formal" (for Lei forms)
- "herbal tea, not tè" (for word choice)
- "motion verb" (for auxiliary selection)

Bad hints:
- Hints that just repeat grammar rules
- Hints that reveal the answer
- Unnecessary hints when context is clear

## ITALIAN LANGUAGE STANDARDS
{lang_prefs}

## COMMON ERRORS TO AVOID

1. **Weekday translations**: "Il sabato" = "On Saturdays" (NOT "On Saturday")
2. **Number agreement**: ottant'anni (NOT ottanta anni) in speech
3. **Article contractions**: Always use del, della, nel, etc. (NOT di il, di la, ne il)
4. **Auxiliary selection**: Check motion/state verbs carefully
5. **Agreement**: Past participles with essere must agree

{topic_notes_section}

## FILL-IN-THE-BLANK RULES

- ONE blank written as ___ (exactly three underscores)
- The correct option must create a perfect sentence when inserted
- Include hint ONLY when truly needed for disambiguation
- Test all options to ensure only one is grammatically correct

## "WRITE:" TRANSLATION RULES

- Start with "Write: " followed by English to translate
- Focus on: common phrases, idiomatic expressions, grammar patterns
- "complete_sentence" contains the perfect Italian translation
- "alternate_correct_responses" lists other acceptable versions
- All options should attempt the translation (even if wrong)

## TOPICS TO COVER

Create AT LEAST the specified number for each topic:
{chr(10).join(f"- {topic}: minimum {count} question(s)" for topic, count in plan_counts.items())}

{'Note: Some topics are marked as challenging and may receive bonus questions.' if any(t in DIFFICULT_TOPICS for t in plan_counts) else ''}

## AVOID THESE EXISTING QUESTIONS

Do not duplicate or closely resemble:
{prior_block}

## QUALITY CHECKLIST

Before including each question, verify:
✓ Complete sentence is natural, idiomatic Italian
✓ English translation is perfectly accurate (including plural markers)
✓ Hint is minimal and only if essential
✓ Resource URL is specific to the topic when available
✓ Explanation cites the specific grammar rule
✓ Level {level} appropriate vocabulary and grammar

Generate the {total_questions} questions now. Return ONLY valid JSON."""

    return prompt

# [REST OF VALIDATION, CSV, UI, AND MAIN WORKFLOW CODE IDENTICAL TO PREVIOUS - CONTINUING WITH SAME FUNCTIONS]

def validate_question(item: dict, level: str, index: int) -> List[str]:
    """Comprehensive validation of a single question."""
    errors = []
    
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in item or item[field] is None:
            errors.append(f"Missing field: {field}")
            return errors
        if isinstance(item[field], str):
            item[field] = item[field].strip()
    
    qt = item["question_text"]
    cs = item["complete_sentence"]
    
    # Determine question type
    is_write = qt.startswith("Write: ")
    
    if is_write:
        if "___" in qt:
            errors.append("Write: questions must not contain ___")
        
        english_part = qt[7:].strip()
        if not english_part:
            errors.append("Write: question missing English text")
        
        if not cs:
            errors.append("Write: question missing Italian translation in complete_sentence")
            
    else:
        blank_count = qt.count("___")
        if blank_count != 1:
            errors.append(f"Must have exactly one ___ blank (found {blank_count})")
        
        correct_opt = item["correct_option"].upper()
        if correct_opt not in {"A", "B", "C", "D"}:
            errors.append(f"Invalid correct_option: {correct_opt}")
        else:
            correct_answer = item[f"option_{correct_opt.lower()}"]
            reconstructed = qt.replace("___", correct_answer)
            if reconstructed != cs:
                errors.append(f"Reconstruction failed. Expected: '{cs}', Got: '{reconstructed}'")
    
    # Validate options
    options = [item[f"option_{x}"] for x in ["a", "b", "c", "d"]]
    if len(set(options)) < 4:
        errors.append("Duplicate options found")
    
    if any(not opt for opt in options):
        errors.append("Empty option found")
    
    # Validate level
    if item["cefr_level"] != level:
        errors.append(f"Wrong level: {item['cefr_level']} (expected {level})")
    
    # Validate topic
    if item["topic"] not in TOPICS_BY_LEVEL[level]:
        errors.append(f"Invalid topic: {item['topic']}")
    
    # Check for common errors
    eng = item["english_translation"].lower()
    
    # Weekday error check
    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for day in weekdays:
        if f"on {day}" in eng and cs.startswith("Il ") and day + "s" not in eng:
            errors.append(f"Habitual weekday should be 'On {day}s' not 'On {day}'")
    
    # Hint validation
    if not is_write and item["hint"]:
        hint_lower = item["hint"].lower()
        for opt in options:
            if hint_lower == opt.lower():
                errors.append("Hint reveals an answer option")
    
    return errors

def validate_and_clean(batch: List[dict], level: str) -> Tuple[List[QA], List[str]]:
    """Validate batch and return only perfect questions."""
    all_errors = []
    cleaned: List[QA] = []
    
    for i, item in enumerate(batch, 1):
        errors = validate_question(item, level, i)
        
        if errors:
            all_errors.append(f"Question {i}: " + "; ".join(errors))
            continue
        
        # Clean and normalize
        for field in REQUIRED_FIELDS:
            if isinstance(item[field], str):
                item[field] = item[field].strip()
            elif item[field] is None:
                item[field] = ""
        
        # Ensure correct_option is uppercase
        item["correct_option"] = item["correct_option"].upper()
        
        # Add resource URL if generic
        if item["resource"] == "https://www.lawlessitalian.com/grammar/":
            item["resource"] = get_resource_url(item["topic"])
        
        cleaned.append(QA(**{k: item[k] for k in REQUIRED_FIELDS}))
    
    return cleaned, all_errors

def append_csv(level: str, items: List[QA]) -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    path = OUTDIR / f"italian_{level}.csv"
    file_exists = path.exists()
    
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REQUIRED_FIELDS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()
        for qa in items:
            writer.writerow(asdict(qa))

def update_coverage(level: str, items: List[QA], state: Dict) -> None:
    cov = state["coverage"].setdefault(level, {})
    for qa in items:
        cov[qa.topic] = cov.get(qa.topic, 0) + 1

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def copy_to_clipboard(text: str) -> bool:
    try:
        pyperclip.copy(text)
        return True
    except:
        return False

def print_separator(char="=", width=80):
    print(char * width)

def save_prompt_to_file(prompt: str, level: str) -> Path:
    PROMPT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROMPT_DIR / f"prompt_{level}_{timestamp}.txt"
    filepath.write_text(prompt, encoding="utf-8")
    return filepath

def manual_bridge_claude(levels: List[str], target: int, batch_size: int):
    """Manual workflow optimized for Claude."""
    state = load_state()
    
    for level in levels:
        seen = load_existing_texts(level)
        done = len(seen)
        
        clear_screen()
        print_separator("=")
        print(f"  ITALIAN MCQ GENERATOR - Level {level} (COMPLETE COMPREHENSIVE)")
        print(f"  Progress: {done}/{target} questions")
        print(f"  Topics: {len(TOPICS_BY_LEVEL[level])}")
        print_separator("=")
        
        while done < target:
            remaining = target - done
            current_batch = min(batch_size, remaining)
            
            # Show coverage stats
            print("\n" + get_coverage_stats(level, state))
            print(f"\n📝 Generating batch of {current_batch} questions...")
            
            # Plan topics
            topics, plan_counts = plan_topics(level, state, current_batch)
            print(f"Topics for this batch: {', '.join(set(topics))}")
            
            # Build prompt
            prompt = build_claude_prompt(level, topics, current_batch, state)
            
            # Save and potentially copy prompt
            prompt_file = save_prompt_to_file(prompt, level)
            print(f"✅ Prompt saved to: {prompt_file}")
            
            if copy_to_clipboard(prompt):
                print("📋 Prompt copied to clipboard!")
            
            print("\n" + "="*80)
            print("INSTRUCTIONS:")
            print("1. The prompt has been copied to your clipboard (or open the saved file)")
            print("2. Paste it into Claude")
            print("3. Copy Claude's JSON response")
            print("4. Paste it here and press Enter twice when done")
            print("="*80 + "\n")
            
            if not copy_to_clipboard(prompt):
                print("PROMPT FOR CLAUDE:")
                print("-"*80)
                print(prompt[:500] + "..." if len(prompt) > 500 else prompt)
                print("-"*80)
                print("\n(Full prompt saved to file - see path above)")
            
            print("\n📋 Paste Claude's JSON response below (press Enter twice when done):\n")
            
            # Read multiline input
            lines = []
            empty_count = 0
            while True:
                try:
                    line = input()
                    if line == "":
                        empty_count += 1
                        if empty_count >= 2:
                            break
                    else:
                        empty_count = 0
                        lines.append(line)
                except EOFError:
                    break
            
            raw = "\n".join(lines).strip()
            if not raw:
                print("❌ No input received. Please try again.")
                input("\nPress Enter to continue...")
                continue
            
            # Parse JSON
            try:
                # Clean potential markdown formatting
                if "```json" in raw:
                    raw = raw.split("```json")[1].split("```")[0]
                elif "```" in raw:
                    raw = raw.split("```")[1].split("```")[0]
                
                payload = json.loads(raw)
                if not isinstance(payload, dict) or "questions" not in payload:
                    raise ValueError("Response must be a JSON object with 'questions' array")
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON: {e}")
                print("Please ensure you copied the complete JSON response from Claude.")
                input("\nPress Enter to retry this batch...")
                continue
            except ValueError as e:
                print(f"❌ Format error: {e}")
                input("\nPress Enter to retry this batch...")
                continue
            
            # Validate and clean
            raw_items = payload.get("questions", [])
            cleaned, errs = validate_and_clean(raw_items, level)
            
            if errs:
                print(f"\n⚠️ Validation issues found ({len(errs)} errors):")
                for e in errs[:5]:
                    print(f"  • {e}")
                if len(errs) > 5:
                    print(f"  ... and {len(errs) - 5} more")
                
                if len(cleaned) == 0:
                    print("\n❌ All questions failed validation. Please regenerate with Claude.")
                    input("\nPress Enter to continue...")
                    continue
            
            # Filter duplicates
            unique_items = []
            duplicates = 0
            for qa in cleaned:
                key = qa.question_text.strip()
                if key not in seen:
                    seen.add(key)
                    unique_items.append(qa)
                else:
                    duplicates += 1
            
            if not unique_items:
                print("❌ All questions were duplicates. Please generate another batch.")
                input("\nPress Enter to continue...")
                continue
            
            # Save to CSV
            append_csv(level, unique_items)
            update_coverage(level, unique_items, state)
            save_state(state)
            
            # Count question types
            write_qs = sum(1 for q in unique_items if q.question_text.startswith("Write: "))
            fill_qs = len(unique_items) - write_qs
            hints_count = sum(1 for q in unique_items if q.hint and not q.question_text.startswith("Write: "))
            
            # Report results
            added = len(unique_items)
            done += added
            
            print(f"\n✅ SUCCESS!")
            print(f"  • Added: {added} new questions ({fill_qs} fill-in, {write_qs} translation)")
            if fill_qs > 0:
                print(f"  • Hints: {hints_count}/{fill_qs} fill-in questions ({hints_count*100//fill_qs}%)")
            print(f"  • Rejected: {len(raw_items) - len(cleaned)} failed validation")
            print(f"  • Duplicates: {duplicates} skipped")
            print(f"  • Progress: {done}/{target} total")
            
            topics_covered = sorted(set(q.topic for q in unique_items))
            if len(topics_covered) <= 5:
                print(f"  • Topics: {', '.join(topics_covered)}")
            else:
                print(f"  • Topics: {len(topics_covered)} different topics covered")
            
            # Prepare next prompt immediately
            if done < target:
                next_batch = min(batch_size, target - done)
                next_topics, _ = plan_topics(level, state, next_batch)
                next_prompt = build_claude_prompt(level, next_topics, next_batch, state)
                
                if copy_to_clipboard(next_prompt):
                    print(f"\n📋 Next prompt already copied to clipboard! (Batch size: {next_batch})")
                
                input("\nPress Enter to continue to next batch...")
                clear_screen()
                print_separator("=")
                print(f"  ITALIAN MCQ GENERATOR - Level {level} (COMPLETE COMPREHENSIVE)")
                print(f"  Progress: {done}/{target} questions")
                print(f"  Topics: {len(TOPICS_BY_LEVEL[level])}")
                print_separator("=")
        
        print(f"\n🎉 Level {level} complete! ({done} questions)")
        if levels.index(level) < len(levels) - 1:
            input("\nPress Enter to continue to next level...")

def dry_run(levels: List[str], batch_size: int):
    """Preview prompts and coverage without generating."""
    state = load_state()
    
    print("\n=== DRY RUN MODE ===")
    print(f"Batch size: {batch_size}")
    
    for level in levels:
        print(f"\n--- Level {level} ---")
        print(f"Total topics: {len(TOPICS_BY_LEVEL[level])}")
        print(get_coverage_stats(level, state))
        
        topics, plan_counts = plan_topics(level, state, batch_size)
        prompt = build_claude_prompt(level, topics, batch_size, state)
        
        print(f"\nTopics planned: {', '.join(set(topics))}")
        print(f"Question distribution:")
        for topic, count in plan_counts.items():
            marker = " [DIFFICULT]" if topic in DIFFICULT_TOPICS else ""
            print(f"  - {topic}: {count} questions{marker}")
        
        print(f"\nPrompt length: {len(prompt)} characters")
        
        # Save to file
        prompt_file = save_prompt_to_file(prompt, level)
        print(f"Prompt saved to: {prompt_file}")
        
        if copy_to_clipboard(prompt):
            print("📋 Prompt copied to clipboard!")
        
        if input("\nShow full prompt? (y/n): ").lower() == 'y':
            print("\n" + prompt)
    
    print("\n✅ Dry run complete. Prompts saved to 'prompts' directory.")

def parse_args():
    ap = argparse.ArgumentParser(
        description="Italian MCQ dataset builder - COMPLETE COMPREHENSIVE ALL LEVELS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
COMPLETE COMPREHENSIVE COVERAGE (217 topics total):
  A1: 46 topics (40 original + 6 new)
      New: Irregular verbs, conjunctions, quantifiers, emotions, phone
  A2: 48 topics (47 original + 1 new)
      New: Borrowing/lending
  B1: 42 topics (39 original + 3 new)
      New: Pets & vet, property boundaries, advanced borrowing
  B2: 43 topics (29 original + 14 new)
      New: Finance, insurance, property, automotive, health, cooking,
           legal, home/garden, fitness, pets, conversation management,
           conflict resolution + 2 grammar topics
  C1: 38 topics (unchanged)

DETAILED NUANCES:
  A2: 15 topics with detailed teaching instructions
  B1: 12 topics with detailed teaching instructions
  B2: 16 topics with detailed teaching instructions (including conversation management)

Examples:
  %(prog)s --dry-run --levels A1 --batch-size 10
  %(prog)s --levels A1,A2 --target 1500 --batch-size 8
  %(prog)s --levels B2 --batch-size 5  # For maximum quality
        """
    )
    ap.add_argument("--levels", type=str, default="A1,A2,B1,B2,C1",
                    help="Comma-separated CEFR levels (default: all)")
    ap.add_argument("--target", type=int, default=TARGET_PER_LEVEL,
                    help=f"Target questions per level (default: {TARGET_PER_LEVEL})")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                    help=f"Questions per batch - reduce for quality (default: {DEFAULT_BATCH_SIZE})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview prompts and coverage without generating")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    # Parse and validate levels
    levels = [s.strip().upper() for s in args.levels.split(",") if s.strip()]
    for l in levels:
        if l not in LEVELS:
            print(f"❌ Unknown level '{l}'. Valid: {', '.join(LEVELS)}")
            sys.exit(1)
    
    print("🚀 Italian MCQ Generator - COMPLETE COMPREHENSIVE ALL LEVELS")
    print(f"   Total Topics: 217 across all levels")
    print(f"   Levels: {', '.join(levels)}")
    print(f"   Target: {args.target} questions per level")
    print(f"   Batch size: {args.batch_size} (reduce for higher quality)")
    print(f"   Mode: {'DRY RUN' if args.dry_run else 'MANUAL GENERATION'}")
    print(f"\n   Coverage per level:")
    for level in LEVELS:
        print(f"     {level}: {len(TOPICS_BY_LEVEL[level])} topics")
    
    if args.batch_size > 10:
        print("\n⚠️  Warning: Batch sizes > 10 may reduce quality. Consider using --batch-size 5-8")
    
    if args.dry_run:
        dry_run(levels, args.batch_size)
    else:
        print("\n📌 Note: This script prioritizes 100% accuracy over speed.")
        print("   Invalid questions will be completely rejected.")
        input("\nPress Enter to begin...")
        manual_bridge_claude(levels, args.target, args.batch_size)
        print("\n✅ All done! Check the 'data' directory for your CSV files.")