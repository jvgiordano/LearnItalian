#!/usr/bin/env python3
"""
Italian MCQ Dataset Generator - Complete Comprehensive All Levels
Produces high-quality Italian language learning questions with 100% accuracy focus.
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import pyperclip
from collections import Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple, Set
from datetime import datetime

# --------------------------------
# Config
# --------------------------------
DEFAULT_BATCH_SIZE = 5
TARGET_PER_LEVEL = 3250
MIN_PER_TOPIC = 15
OUTDIR = Path(".")
STATE_PATH = Path("state_progress_comprehensive.json")
PROMPT_DIR = Path("prompts")
LEVELS = ["A1", "A2", "B1", "B2", "C1"]

WRITE_RATIO = 0.05
BONUS_RATIO = 0.20
BONUS_MAX = 2

# --------------------------------
# Topics by Level
# --------------------------------
TOPICS_BY_LEVEL: Dict[str, List[str]] = {
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
        "Verbi impersonali",
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

# --------------------------------
# Topic nuances
# --------------------------------
TOPIC_NUANCES = {
    # ==================== A1 NUANCES ====================

    "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)": """
    Essential high-frequency irregular verbs - must be mastered at A1:
    ANDARE: vado, vai, va, andiamo, andate, vanno
    FARE: faccio, fai, fa, facciamo, fate, fanno
    VENIRE: vengo, vieni, viene, veniamo, venite, vengono
    STARE: sto, stai, sta, stiamo, state, stanno
    DARE: do, dai, dà, diamo, date, danno
    USCIRE: esco, esci, esce, usciamo, uscite, escono
    """,

    "Date (esprimere la data)": """
    Italian dates use: il + cardinal number + month (+ year).
    "Il 3 maggio 2024" (May 3rd, 2024).

    SOLE EXCEPTION — first of the month uses ordinal:
    "Il primo gennaio" (January 1st) — NEVER "l'uno gennaio".

    Day-before-month order (opposite of American English):
    il 15 marzo = March 15th.

    Asking the date:
    "Quanti ne abbiamo oggi?" / "Che data è oggi?"
    "Oggi è il 10 aprile." / "Ne abbiamo 10."

    Prepositions with dates and years:
    • nel + year: "nel 2024" (in 2024) — no article after nel
    • Standalone year takes article: "il 2024 è stato un buon anno"
    • il + day + month: "il 25 dicembre" (on December 25th)

    Common errors:
    • *"l'uno marzo" → "il primo marzo" (only primo for 1st)
    • *"maggio 3" → "il 3 maggio" (day before month)
    • Writing dates: 15/03/2024 (DD/MM/YYYY in Italy)
    """,

    "Numeri cardinali 0–1000+ e prezzi": """
    Extended cardinal numbers with spelling rules:

    ELISION IN COMPOUNDS:
    • ventuno, trentuno, quarantuno... drop final vowel before a noun:
      "ventun anni" (21 years), NOT "ventuno anni"
    • Same with otto: "ventotto" (28), "trentotto" (38)

    HUNDREDS:
    • cento (100) — invariable, no article: "cento euro" NOT "un cento euro"
    • duecento (200), trecento (300)... quattrocento, cinquecento, seicento,
      settecento, ottocento, novecento

    THOUSANDS:
    • mille (1000) — irregular plural: mille → duemila, tremila, quattromila...
    • No article: "mille euro" NOT "un mille euro"

    PRICES:
    • "Costa tre euro e cinquanta (centesimi)" — €3.50
    • Italy uses comma for decimals: 3,50 € (not 3.50)
    • "Quanto costa?" / "Quanto viene?"

    Common errors:
    • *"un cento" → "cento" (no article before cento)
    • *"un mille" → "mille" (no article before mille)
    • *"due mille" → "duemila" (one word, mille→mila in plural)
    • *"ventuno anni" → "ventun anni" (elision before noun)
    """,

    # ==================== A2 NUANCES ====================

    "Imperfetto vs. Passato Prossimo": """
    MUST cover: completed vs ongoing, specific vs habitual, background vs main event.
    Time signals: sempre/spesso/di solito → imperfetto; ieri/lunedì scorso → passato prossimo.
    Modal verbs differ: "Volevo uscire" (ongoing desire) vs "Ho voluto uscire" (decided).
    """,
    "Passato prossimo (reg/irr; essere/avere; accordo)": """
    Focus: auxiliary selection, agreement with essere, common irregular participles
    (fatto, stato, detto, visto, scritto), motion verbs → essere, reflexives → essere.
    No agreement with avere unless direct object pronoun precedes.
    """,
    "Pronomi Combinati": """
    mi→me, ti→te, ci→ce, vi→ve before direct pronoun.
    gli/le/Le → glie- (glielo/gliela/glieli/gliele).
    Order: INDIRECT + DIRECT. Attach to infinitives and positive imperatives.
    """,
    "Pronomi Relativi": """
    che (subject/direct object), cui (after prepositions), il quale (formal),
    chi (those who). Examples: "il libro che leggo", "la persona di cui parlo".
    """,
    "Si Impersonale": """
    Si + 3rd person singular for general "one/people/we".
    Agreement with plural nouns for si passivante: "Si vendono case".
    """,
    "Imperativo Formale": """
    Uses congiuntivo presente for Lei: Venga! Faccia! Sia! Vada!
    With pronouns: "Me lo dia!" Negative: "Non venga!"
    """,
    "Volerci vs. Metterci": """
    Volerci = impersonal (time/things needed): "Ci vuole un'ora".
    Metterci = personal (time someone takes): "Ci metto un'ora".
    """,
    "Stare + Gerundio": """
    Right-now actions only. Irregular gerunds: fare→facendo, dire→dicendo, bere→bevendo.
    Pronouns: "Lo sto mangiando" OR "Sto mangiandolo".
    """,
    "Condizionale Presente": """
    Polite requests (Vorrei, Potrebbe), hypotheticals, advice (Dovresti).
    Irregular stems same as future: essere→sarei, avere→avrei, fare→farei.
    """,
    "Forme negative composte (non...mai, non...niente, ecc.)": """
    Italian REQUIRES double negatives. mai, niente/nulla, nessuno, più, ancora.
    Emphatic: "Mai vado al cinema" (rare, literary/emphatic).
    """,
    "Congiunzioni e connettivi (base)": """
    Coordination: e, o, ma, però. Cause: perché, siccome.
    Effect: quindi, allora, perciò. Time: quando, mentre, prima di, dopo.
    """,
    "Numeri ordinali": """
    primo–decimo irregular; from 11th add -esimo. Agreement required.
    Dates: ONLY "il primo" for 1st, cardinals for rest. Floors, centuries, rankings.
    """,
    "Preposizioni articolate": """
    Mandatory contractions: di+il=del, a+il=al, da+il=dal, in+il=nel, su+il=sul etc.
    NEVER say "di il" — always contract.
    """,
    "Pronomi diretti": """
    mi, ti, lo/la, ci, vi, li/le. Before conjugated verb or attached to infinitive.
    Agreement in passato prossimo: "L'ho vista". La = formal you.
    """,
    "Pronomi indiretti": """
    mi, ti, gli/le, ci, vi, gli. No agreement in passato prossimo.
    Common with: dare, dire, telefonare, chiedere, rispondere, scrivere, regalare.
    """,
    "Prestare e chiedere in prestito": """
    PRESTARE = lend (you give): "Mi presti la macchina?"
    PRENDERE IN PRESTITO = borrow (you receive): "Posso prendere in prestito il tuo telefono?"
    RESTITUIRE = return: "Ti restituisco il libro domani."
    """,

    "Forme speciali di bello, buono, grande (pre-nominali)": """
    When placed BEFORE a noun, these adjectives follow special patterns.
    After the noun, they use regular forms (bello, buono, grande).

    BELLO (follows pattern of definite article il/lo/la/i/gli/le):
    • bel + consonant: un bel giorno, un bel ragazzo
    • bell' + vowel: un bell'uomo, una bell'idea
    • bello + s+consonant/z/gn/ps: un bello spettacolo, un bello zaino
    • bella + fem: una bella donna
    • bei + masc pl consonant: bei fiori, bei tempi
    • begli + masc pl vowel/s+cons/z: begli occhi, begli studenti
    • belle + fem pl: belle ragazze

    BUONO (follows pattern of indefinite article un/uno/una/un'):
    • buon + masc consonant/vowel: un buon amico, un buon libro
    • buono + s+consonant/z: un buono studente
    • buona + fem consonant: una buona idea
    • buon' + fem vowel: buon'amica (less common today, buona amica also accepted)
    • buoni/buone in plural (regular)

    GRANDE (truncation):
    • gran + most consonants: un gran problema, una gran fortuna
    • grand' + vowel: una grand'idea (or grande idea — both accepted)
    • grande before s+consonant/z: un grande spettacolo
    • grandi in plural (regular)

    FIXED EXPRESSIONS:
    buon giorno, buon appetito, buon viaggio, buona fortuna,
    buona notte, gran parte, gran che, un bel po'

    Post-nominal = regular: "un libro bello" "un amico buono" "un problema grande"
    """,

    "Plurali irregolari (nomi con plurale irregolare)": """
    Many high-frequency Italian nouns have irregular plurals.

    MASCULINE SINGULAR → FEMININE PLURAL (body parts & others):
    • il braccio → le braccia (arms)
    • il dito → le dita (fingers)
    • il ginocchio → le ginocchia (knees)
    • il labbro → le labbra (lips)
    • l'uovo → le uova (eggs)
    • il paio → le paia (pairs)
    • il miglio → le miglia (miles)
    • il lenzuolo → le lenzuola (sheets)
    Pattern: many body-part nouns follow this masc-sg → fem-pl rule.

    INVARIABLE NOUNS (same form singular and plural):
    • Truncated words: la foto → le foto, la radio → le radio, l'auto → le auto,
      la moto → le moto, il cinema → i cinema
    • Words ending in accented vowel: la città → le città, l'università → le università,
      il caffè → i caffè
    • Foreign loans: il bar → i bar, il film → i film, lo sport → gli sport,
      il computer → i computer

    SPELLING CHANGES (preserving pronunciation):
    • l'amico → gli amici (hard /k/ preserved)
    • il medico → i medici
    • l'amica → le amiche (hard /k/ preserved with -he)

    IRREGULAR MASCULINE PLURALS:
    • l'uomo → gli uomini
    • il dio → gli dèi

    ALWAYS PLURAL / ALWAYS SINGULAR:
    • Always plural: le forbici (scissors), gli occhiali (glasses),
      i pantaloni (trousers), le nozze (wedding)
    • Always singular: la gente (people — collective, takes singular verb)

    TRICKY GENDER:
    • la mano → le mani (feminine despite -o ending!)
    • la chiave → le chiavi (feminine)
    • l'acqua → le acque (feminine; plural used in: acque minerali, le acque del fiume)
    """,

    # ==================== B1 NUANCES ====================

    "Trapassato prossimo": """
    Formation: imperfetto of essere/avere + past participle.
    Sequence: "Quando sono arrivato, Maria era già partita."
    Agreement rules same as passato prossimo.
    """,
    "Congiuntivo presente (introduzione: opinioni, emozioni base)": """
    After: penso che, credo che, mi sembra che, sono felice che, ho paura che,
    è importante che, bisogna che. NOT after: so che (certainty).
    Common irregulars: sia, abbia, faccia, vada.
    """,
    "Accordo del participio passato (con pronomi diretti, ne, riflessivi)": """
    essere → always agree. Direct object pronouns → agree (L'ho vista).
    ne → agree with replaced noun. Reflexives → agree with subject.
    avere + post-verb direct object → NO agreement.
    """,
    "Periodo ipotetico I tipo": """
    Se + presente indicativo → futuro/imperativo/presente.
    Real/likely conditions. "Se piove, resterò a casa."
    """,
    "Si impersonale / si passivante (base)": """
    Impersonale: active focus, si + 3rd sg. "Si mangia bene qui."
    Passivante: passive focus, agrees with noun. "Si vendono case."
    """,
    "Discorso indiretto (base)": """
    Tense shifts when reporting verb is past: presente→imperfetto, pp→trapassato, futuro→condizionale passato.
    Time: oggi→quel giorno, domani→il giorno dopo.
    """,
    "Ci e ne (avanzato)": """
    CI: place, about it (pensarci, crederci), replaces a+noun.
    NE: quantity (Ne voglio tre), about it, replaces di+noun, partitive.
    Combined: me ne, te ne, se ne (Me ne vado).
    """,
    "Passivo con essere (tempi principali)": """
    essere (any tense) + past participle agreeing with subject.
    Agent introduced by da. Alternative: si passivante.
    """,
    "Comparativi e superlativi (irregolarità)": """
    buono→migliore/ottimo, cattivo→peggiore/pessimo, grande→maggiore/massimo, piccolo→minore/minimo.
    di vs che: "più alto di Luca" (two things) vs "più facile che difficile" (two qualities).
    """,
    "Gerundio (usi)": """
    Cause (essendo stanco), time (camminando), condition (studiando), manner (correndo).
    After pur: "pur essendo". Pronouns attach to end.
    """,
    "Pronomi combinati (glielo, me ne, ecc.)": """
    Indirect changes: mi→me, ti→te, ci→ce, vi→ve. gli/le → glie-.
    Imperatives: attach positive (Dammelo!), separate negative (Non me lo dare!).
    """,
    "Sapere vs. Conoscere": """
    SAPERE: facts, information, skills (+ infinitive). so, sai, sa, sappiamo, sapete, sanno.
    CONOSCERE: people, places, familiarity (+ noun only). Never with infinitive.
    """,
    "Preposizioni + infinito (da, di, a)": """
    DA: qualcosa da fare, facile da capire.
    DI: cercare di, decidere di, finire di, essere felice di.
    A: andare a, cominciare a, imparare a, riuscire a.
    Modal verbs take NO preposition.
    """,
    "Suffissi (diminutivi, accrescitivi, peggiorativi)": """
    Diminutives: -ino, -etto, -ello, -uccio. Augmentatives: -one (can change gender).
    Pejoratives: -accio, -astro. Can combine: casettina. Very productive in spoken Italian.
    """,
    "Pronomi possessivi (vs aggettivi possessivi)": """
    Adjective: il mio libro. Pronoun (standalone): il mio (mine) — keeps article.
    With essere: article optional ("È mio" / "È il mio").
    loro: invariable (il loro/la loro/i loro/le loro).
    """,
    "Infinito passato (dopo aver/essere + participio)": """
    avere/essere (infinitive) + past participle. "Dopo aver mangiato, sono uscito."
    Agreement with essere subject. Same subject required as main clause.
    """,
    "Animali domestici e veterinario": """
    Pets: cane, gatto, pesce, uccellino, coniglio, criceto.
    Vet vocab: vaccinazioni, antirabbica, antipulci, vermifugo, visita, richiamo.
    Common phrases: "Il gatto non sta bene", "Quanto costa la visita?"
    """,
    "Proprietà e confini": """
    Assertive commands: "Questa è proprietà privata", "Deve andarsene" (formal), "Vattene!" (informal).
    Legal: vietato l'accesso, intrusione, chiamare la polizia.
    Escalation: "Se non se ne va, chiamo la polizia."
    """,

    "Comunicazione efficace e influenza sociale": """
    Effective communication and social influence — the foundations of
    persuasion, rapport, and social intelligence expressed in Italian.
    B1 grammar: condizionale for politeness, basic congiuntivo for
    opinions, connectors for cause/effect, comparatives, indirect speech.

    ═══════════════════════════════════════════════════════════════
    PART 1: GENUINE INTEREST AND LISTENING (Carnegie)
    ═══════════════════════════════════════════════════════════════

    Carnegie's core insight: people care most about themselves and
    their own problems. The most influential skill is making others
    feel heard, valued, and important — sincerely, not manipulatively.

    1A. SHOWING GENUINE INTEREST IN OTHERS
    The fastest way to connect is to be genuinely curious about
    the other person rather than trying to be interesting yourself.

    • "Mi piacerebbe sapere cosa ne pensi tu"
      (I'd like to know what you think about it)
    • "Come hai cominciato a interessarti di questo?"
      (How did you start getting interested in this?)
    • "Che cosa ti ha spinto a fare questa scelta?"
      (What drove you to make this choice?)
    • "Raccontami di più — mi interessa davvero"
      (Tell me more — I'm really interested)
    • "Com'è stato per te?"
      (What was that like for you?)
    • "Cosa ti piace di più del tuo lavoro?"
      (What do you like most about your work?)

    Principle: Ask questions that let people talk about what
    matters to them. Then actually listen to the answer.

    1B. ACTIVE LISTENING AND MAKING OTHERS FEEL HEARD
    Listening is not waiting for your turn to speak.
    Reflect back what you hear to show understanding.

    • "Se ho capito bene, stai dicendo che..."
      (If I understand correctly, you're saying that...)
    • "Quindi per te la cosa più importante è..."
      (So for you the most important thing is...)
    • "Mi sembra che questo ti stia molto a cuore"
      (It seems like this is very close to your heart)
    • "Capisco perché la pensi così"
      (I understand why you think that way)
    • "È interessante — non ci avevo mai pensato da questo punto di vista"
      (That's interesting — I'd never thought about it from that angle)
    • "Quello che dici ha molto senso"
      (What you're saying makes a lot of sense)

    Principle: Paraphrase and validate before responding.
    People don't listen to those who haven't first listened to them.

    1C. REMEMBERING AND USING NAMES
    A person's name is, to that person, the most important
    sound in any language.

    • "Piacere di conoscerti, Marco — Marco, vero?"
      (Nice to meet you, Marco — Marco, right?)
    • "Come hai detto che ti chiami? Voglio ricordarmelo"
      (What did you say your name was? I want to remember it)
    • "Marco, volevo chiederti una cosa"
      (Marco, I wanted to ask you something)

    Principle: Use the person's name naturally in conversation.
    It signals respect and creates instant connection.

    ═══════════════════════════════════════════════════════════════
    PART 2: HONEST APPRECIATION VS FLATTERY (Carnegie)
    ═══════════════════════════════════════════════════════════════

    Flattery is cheap and people detect it. Honest appreciation
    is specific, sincere, and focused on something real.

    2A. GIVING SINCERE APPRECIATION
    Be specific about what you admire. Vague praise sounds hollow.

    • "Ammiro il modo in cui hai gestito quella situazione"
      (I admire the way you handled that situation)
    • "Hai un talento vero per spiegare le cose in modo semplice"
      (You have a real talent for explaining things simply)
    • "Si vede che ci hai messo molto impegno — il risultato è ottimo"
      (You can tell you put a lot of effort in — the result is excellent)
    • "Quello che hai fatto non era facile, e l'hai fatto bene"
      (What you did wasn't easy, and you did it well)
    • "Ho imparato molto da come hai affrontato il problema"
      (I learned a lot from how you approached the problem)

    Bad (flattery): "Sei bravissimo!" (generic, sounds hollow)
    Good (appreciation): "Il modo in cui hai risolto quel conflitto
    con il cliente è stato davvero efficace" (specific, sincere)

    2B. GIVING PEOPLE A REPUTATION TO LIVE UP TO
    When you attribute a quality to someone, they work to prove
    you right. This is not manipulation — it is seeing the best
    in people and telling them you see it.

    • "So che sei una persona corretta, quindi sono sicuro che
       troveremo una soluzione giusta"
      (I know you're a fair person, so I'm sure we'll find a fair solution)
    • "Tu sei sempre stato uno che mantiene la parola"
      (You've always been someone who keeps their word)
    • "Conosco la tua attenzione ai dettagli — per questo
       ti chiedo questo favore"
      (I know your attention to detail — that's why I'm asking you this favour)

    Principle: People rise to meet the expectations set for them.

    ═══════════════════════════════════════════════════════════════
    PART 3: DISAGREEING WITHOUT CREATING ENEMIES (Carnegie)
    ═══════════════════════════════════════════════════════════════

    You cannot win an argument. Even if you prove someone wrong,
    they will resent you. The goal is to influence, not to "win."

    3A. AVOIDING DIRECT CONTRADICTION
    Never say "Hai torto" (You're wrong). Instead, show respect
    for their view while introducing your own.

    • "Capisco il tuo punto di vista, ma forse potremmo
       considerare anche un altro aspetto"
      (I understand your point of view, but maybe we could
       also consider another aspect)
    • "Hai ragione su molte cose — c'è solo un punto
       su cui la penso diversamente"
      (You're right about many things — there's just one point
       where I think differently)
    • "Non ci avevo pensato così. Allo stesso tempo, mi chiedo se..."
      (I hadn't thought of it that way. At the same time, I wonder if...)
    • "È un'opinione interessante. Io ho avuto un'esperienza
       un po' diversa..."
      (That's an interesting opinion. I've had a somewhat
       different experience...)
    • "Potrebbe essere come dici tu. Ho letto però che..."
      (It could be as you say. However, I've read that...)

    3B. ADMITTING WHEN YOU'RE WRONG
    Admitting mistakes quickly and emphatically disarms criticism
    and earns respect.

    • "Hai assolutamente ragione — avevo torto io"
      (You're absolutely right — I was wrong)
    • "Mi sono sbagliato/a e me ne scuso"
      (I was mistaken and I apologize for it)
    • "Devo ammettere che non ci avevo pensato bene"
      (I have to admit I hadn't thought it through)
    • "È colpa mia — avrei dovuto fare diversamente"
      (It's my fault — I should have done it differently)

    Principle: When wrong, say so immediately. It takes the
    wind out of the other person's sails and builds trust.

    3C. THE SOCRATIC METHOD — GUIDING WITH QUESTIONS
    Instead of telling someone they're wrong, ask questions
    that let them discover the problem themselves.

    • "Cosa succederebbe se facessimo così?"
      (What would happen if we did it this way?)
    • "Come potremmo verificare se è vero?"
      (How could we verify if that's true?)
    • "Quali sarebbero i rischi di questa scelta?"
      (What would the risks of this choice be?)
    • "Hai pensato a cosa potrebbe andare storto?"
      (Have you thought about what could go wrong?)
    • "Secondo te, qual è il modo migliore per risolvere questo?"
      (In your opinion, what's the best way to solve this?)

    Principle: A conclusion people reach themselves is far more
    powerful than one you hand to them.

    ═══════════════════════════════════════════════════════════════
    PART 4: BASIC SOCIAL PROOF AND RECIPROCITY (Cialdini)
    ═══════════════════════════════════════════════════════════════

    4A. SOCIAL PROOF (what others do influences what we do)

    • "Molte persone in questa situazione scelgono di..."
      (Many people in this situation choose to...)
    • "La maggior parte dei miei colleghi fa così"
      (Most of my colleagues do it this way)
    • "Chi ha provato questo metodo dice che funziona bene"
      (People who have tried this method say it works well)
    • "È una scelta molto comune per chi ha le tue esigenze"
      (It's a very common choice for people with your needs)

    4B. RECIPROCITY (we feel obligated to return what we receive)

    • "Ti mando quelle informazioni intanto — con calma, poi
       ne parliamo"
      (I'll send you that information in the meantime — no rush,
       then we'll talk about it)
    • "Io sono disposto/a a fare questo passo — spero che
       anche tu possa venirmi incontro"
      (I'm willing to take this step — I hope you can also
       meet me halfway)
    • "Ti faccio volentieri questo favore"
      (I'm happy to do you this favour)

    Principle: Genuine generosity — not transactional favours —
    builds influence over time.

    4C. TALKING IN TERMS OF THE OTHER PERSON'S INTERESTS

    • "Questo potrebbe essere utile per il tuo progetto"
      (This could be useful for your project)
    • "Se ti interessa risparmiare tempo, potresti provare..."
      (If you're interested in saving time, you could try...)
    • "Pensavo a te quando ho visto questa opportunità"
      (I was thinking of you when I saw this opportunity)
    • "Come posso aiutarti a raggiungere il tuo obiettivo?"
      (How can I help you reach your goal?)
    • "Cosa ci guadagni tu?" (What do you get out of it?)
      — Always have a clear answer to this from their perspective.

    Principle: People don't care what you want. They care what
    they want. Show them how your idea serves their interests.

    ═══════════════════════════════════════════════════════════════
    PART 5: LETTING THE IDEA BE THEIRS (Carnegie)
    ═══════════════════════════════════════════════════════════════

    Nobody likes being told what to do. If you plant a seed and
    let someone water it, they'll defend the flower as their own.

    • "Tu cosa faresti al mio posto?"
      (What would you do in my place?)
    • "Mi è venuta un'idea partendo da quello che hai detto tu..."
      (I got an idea starting from what you said...)
    • "Forse la tua proposta di prima potrebbe funzionare
       anche per questo problema"
      (Maybe your earlier suggestion could work for this problem too)
    • "Come svilupperesti questa idea?"
      (How would you develop this idea?)
    • "La tua esperienza in questo campo è preziosa —
       come la vedi tu?"
      (Your experience in this area is valuable — how do you see it?)

    Principle: Give credit generously. The person who takes
    credit gets the ego boost. The person who gives credit
    gets the influence.

    ═══════════════════════════════════════════════════════════════
    PART 6: INDIRECT INFLUENCE AND SUGGESTIONS (Carnegie)
    ═══════════════════════════════════════════════════════════════

    When correcting someone or suggesting changes, indirectness
    preserves dignity and openness.

    6A. CALLING ATTENTION TO MISTAKES INDIRECTLY

    • "Il lavoro è ottimo nel complesso. C'è solo un punto
       che potremmo migliorare insieme"
      (The work is excellent overall. There's just one point
       we could improve together)
    • "Ho notato una cosa — probabilmente è una svista"
      (I noticed something — it's probably an oversight)
    • "Anch'io facevo lo stesso errore all'inizio"
      (I used to make the same mistake at the beginning too)

    6B. ASKING INSTEAD OF ORDERING

    • "Ti andrebbe di occuparti di questo?"
      (Would you feel like taking care of this?)
    • "Che ne dici se provassimo un altro approccio?"
      (What do you say if we tried a different approach?)
    • "Potremmo forse considerare di..."
      (Could we perhaps consider...)
    • "Sarebbe utile se qualcuno si occupasse di..."
      (It would be helpful if someone took care of...)

    6C. MAKING THE OTHER PERSON HAPPY ABOUT THE SUGGESTION

    • "Sei l'unico/a che potrebbe fare bene questo lavoro"
      (You're the only one who could do this job well)
    • "So che è una sfida, ma credo che tu sia
       la persona giusta"
      (I know it's a challenge, but I believe you're
       the right person)
    • "Questo compito richiede qualcuno con la tua esperienza"
      (This task requires someone with your experience)

    Principle: Praise first, then suggest. Ask, don't order.
    Make the task feel like an opportunity, not a burden.

    ═══════════════════════════════════════════════════════════════
    PART 7: BEGINNING IN A FRIENDLY WAY / THE YES LADDER
    ═══════════════════════════════════════════════════════════════

    Start conversations with warmth and agreement. Get the other
    person saying "sì" early and often — it creates psychological
    momentum toward agreement.

    • "Siamo d'accordo che il progetto è importante, vero?"
      (We agree that the project is important, right?)
    • "Anche tu vuoi trovare la soluzione migliore, no?"
      (You also want to find the best solution, right?)
    • "Penso che possiamo essere d'accordo su questo punto..."
      (I think we can agree on this point...)
    • "Prima di tutto, voglio dirti che apprezzo il tuo tempo"
      (First of all, I want to tell you I appreciate your time)

    Principle: A person who has said "sì" three times is
    psychologically primed to say "sì" a fourth time.

    ═══════════════════════════════════════════════════════════════
    PART 8: APPEALING TO NOBLER MOTIVES (Carnegie)
    ═══════════════════════════════════════════════════════════════

    People like to think of themselves as fair, generous, and
    principled. Appeal to that self-image.

    • "So che per te la correttezza è importante"
      (I know that fairness is important to you)
    • "Tu sei una persona che fa la cosa giusta"
      (You're a person who does the right thing)
    • "Sono sicuro/a che vorrai fare la scelta più equa"
      (I'm sure you'll want to make the fairest choice)
    • "Conto sulla tua onestà"
      (I'm counting on your honesty)

    Principle: Give people a noble reason and they will often
    live up to it, even if their original motive was selfish.

    ═══════════════════════════════════════════════════════════════
    PART 9: SAVING FACE (Carnegie)
    ═══════════════════════════════════════════════════════════════

    Never back someone into a corner. Always leave them a
    dignified way out, even when you're right.

    • "Capisco che la situazione era difficile"
      (I understand the situation was difficult)
    • "Chiunque avrebbe potuto fare lo stesso errore"
      (Anyone could have made the same mistake)
    • "Non è colpa tua — le informazioni non erano chiare"
      (It's not your fault — the information wasn't clear)
    • "L'importante è come risolviamo il problema adesso"
      (The important thing is how we solve the problem now)
    • "Non parliamo più di chi ha sbagliato — parliamo
       di come andare avanti"
      (Let's stop talking about who made a mistake — let's
       talk about how to move forward)

    Principle: A person who loses face becomes your enemy.
    A person whose dignity you protect becomes your ally.
    """,

    # ==================== B2 NUANCES ====================

    "Pronomi tonici (me stesso, te stesso, sé stesso, ecc.)": """
    Emphasis: "L'ho fatto io stesso." Reflexive: "Pensa solo a se stesso."
    By oneself: "da solo" vs "da me stesso". sé (accented) = stressed form.
    """,
    "Verbi causativi: lasciare (permettere)": """
    LASCIARE = let/allow (person chooses). FARE = make/have (caused to do).
    Lasciare + inf: "Lasciami parlare." Lasciare che + subj: "Lascio che vadano."
    Expressions: "Lascia perdere", "Lascia stare", "Lasciami in pace."
    """,
    "Gestione delle conversazioni difficili": """
    Naming patterns, anchoring/redirecting, spotting deflection, reality-checking,
    setting conversational boundaries, metacognitive checks, addressing dishonesty.
    Key phrases for each strategy included.
    """,
    "Conflitto e risoluzione (avanzato)": """
    Acknowledge without agreeing, separate problem from person, assertive vs aggressive,
    de-escalation, identifying real issue, solution-focused language, taking responsibility.
    Use "I" statements: "Io vedo/penso/sento" not "Tu sei/fai sempre."
    """,

    "Congiuntivo trapassato": """
    Subjunctive pluperfect — required for Periodo ipotetico III and past hypotheticals.

    FORMATION:
    congiuntivo imperfetto of essere/avere + past participle

    With AVERE:                    With ESSERE:
    che io avessi mangiato         che io fossi andato/a
    che tu avessi mangiato         che tu fossi andato/a
    che lui/lei avesse mangiato    che lui/lei fosse andato/a
    che noi avessimo mangiato      che noi fossimo andati/e
    che voi aveste mangiato        che voi foste andati/e
    che loro avessero mangiato     che loro fossero andati/e

    USES:

    1. Periodo ipotetico III (impossible/counterfactual past):
    "Se avessi saputo, sarei venuto" (If I had known, I would have come)
    "Se fossi partito prima, non avrei perso il treno"
    Structure: Se + congiuntivo trapassato → condizionale passato

    2. After expressions of opinion/emotion referring to the past:
    "Credevo che fosse già partito" (I thought he had already left)
    "Speravo che avessero finito" (I hoped they had finished)
    "Era strano che non avesse chiamato" (It was strange that he hadn't called)

    3. After conjunctions requiring subjunctive (past reference):
    "Benché avesse studiato, non ha superato l'esame"
    (Although he had studied, he didn't pass the exam)

    AGREEMENT RULES:
    Same as passato prossimo — essere verbs agree with subject:
    "Credevo che Maria fosse partitA" (feminine)
    "Pensavo che loro fossero arrivatI" (masculine plural)
    """,

    "Condizionale passato": """
    Past conditional — essential for hypotheticals, future-in-the-past, and regrets.

    FORMATION:
    condizionale presente of essere/avere + past participle

    With AVERE:                With ESSERE:
    avrei mangiato             sarei andato/a
    avresti mangiato           saresti andato/a
    avrebbe mangiato           sarebbe andato/a
    avremmo mangiato           saremmo andati/e
    avreste mangiato           sareste andati/e
    avrebbero mangiato         sarebbero andati/e

    USES:

    1. Result clause of Periodo ipotetico III:
    "Se avessi studiato, avrei superato l'esame"
    (If I had studied, I would have passed the exam)
    "Se fossi partito prima, sarei arrivato in tempo"

    2. Future-in-the-past (reported speech):
    "Ha detto che sarebbe venuto" (He said he would come)
    "Pensavo che avresti chiamato" (I thought you would call)
    "Sapevo che non avrebbero accettato" (I knew they wouldn't accept)

    3. Unrealised wishes and regrets:
    "Avrei voluto viaggiare di più" (I would have liked to travel more)
    "Sarei dovuto andare dal medico" (I should have gone to the doctor)
    "Avremmo potuto fare meglio" (We could have done better)

    4. Polite/softened statements about the past:
    "Avrei preferito restare" (I would have preferred to stay)

    AGREEMENT with essere: same rules as passato prossimo.
    "Maria sarebbe partitA" "Loro sarebbero arrivatI"

    Common error: using imperfect instead of condizionale passato
    in reported speech: *"Ha detto che veniva" (colloquial but not standard)
    → "Ha detto che sarebbe venuto" (correct standard form)
    """,

    "Passato remoto (introduzione)": """
    Simple past — the standard narrative past tense in written Italian
    and spoken Italian in central/southern Italy.

    REGULAR CONJUGATION:

    -ARE (parlare):        -ERE (credere):       -IRE (dormire):
    parlai                 credei (credetti)      dormii
    parlasti               credesti               dormisti
    parlò                  credé (credette)       dormì
    parlammo               credemmo               dormimmo
    parlaste               credeste               dormiste
    parlarono              crederono (credettero) dormirono

    Note: -ere verbs often have alternative forms (credei/credetti).

    MOST COMMON IRREGULAR FORMS (must memorise):
    essere: fui, fosti, fu, fummo, foste, furono
    avere: ebbi, avesti, ebbe, avemmo, aveste, ebbero
    fare: feci, facesti, fece, facemmo, faceste, fecero
    dire: dissi, dicesti, disse, dicemmo, diceste, dissero
    venire: venni, venisti, venne, venimmo, veniste, vennero
    vedere: vidi, vedesti, vide, vedemmo, vedeste, videro
    scrivere: scrissi, scrivesti, scrisse, scrivemmo, scriveste, scrissero
    prendere: presi, prendesti, prese, prendemmo, prendeste, presero
    mettere: misi, mettesti, mise, mettemmo, metteste, misero
    chiedere: chiesi, chiedesti, chiese, chiedemmo, chiedeste, chiesero
    rispondere: risposi, rispondesti, rispose, rispondemmo, rispondeste, risposero
    nascere: nacqui, nascesti, nacque, nascemmo, nasceste, nacquero
    vivere: vissi, vivesti, visse, vivemmo, viveste, vissero

    USAGE vs PASSATO PROSSIMO:
    • Passato remoto: events felt as distant/completed, narrative, historical,
      literary. Standard in writing. Dominant in central/southern speech.
    • Passato prossimo: events felt as recent or relevant to present.
      Dominant in northern spoken Italian.
    • Regional variation: a Roman says "Ieri andai" where a Milanese says
      "Ieri sono andato" — both correct.
    • Written Italian: passato remoto is standard for narration
      (novels, history, journalism, biographies).
    """,

    "Negoziazione e persuasione": """
    Advanced negotiation and persuasion — professional, personal,
    and social contexts. B2 grammar: full subjunctive, hypotheticals,
    complex connectors, implicit forms, nuanced register.

    ═══════════════════════════════════════════════════════════════
    PART 1: TACTICAL EMPATHY (Voss)
    ═══════════════════════════════════════════════════════════════

    Tactical empathy is understanding someone's feelings AND
    demonstrating that understanding. It's not agreeing — it's
    showing you see their world. This disarms defensiveness.

    1A. LABELING EMOTIONS
    Name the emotion you observe. Start with "Sembra che..."
    or "Ho l'impressione che..." — never "Tu sei..."

    • "Sembra che questa situazione ti preoccupi molto"
      (It seems like this situation worries you a lot)
    • "Ho l'impressione che tu non ti senta ascoltato/a"
      (I get the impression you don't feel heard)
    • "Pare che ci sia una certa frustrazione riguardo ai tempi"
      (It seems there's some frustration about the timeline)
    • "Mi sembra che per te questo punto sia fondamentale"
      (It seems to me that this point is fundamental for you)
    • "Si direbbe che tu abbia dei dubbi su questa proposta"
      (It would seem you have some doubts about this proposal)
    • "Ho la sensazione che ci sia qualcosa che non mi
       stai dicendo"
      (I have the feeling there's something you're not telling me)

    Principle: Labeling an emotion reduces its intensity.
    When people feel understood, their defences drop.

    1B. MIRRORING (Voss)
    Repeat the last 1-3 critical words of what someone said,
    with an upward inflection. Then go silent. They will expand.

    Person: "Non possiamo accettare queste condizioni."
    You: "...queste condizioni?" [then silence]
    → They will explain WHY, giving you critical information.

    Person: "Il problema è che non abbiamo abbastanza tempo."
    You: "Non abbastanza tempo...?" [silence]
    → Forces them to elaborate without you asking a question.

    • "...non è possibile?"  (repeating their statement)
    • "...troppo rischioso?"
    • "...entro venerdì?"

    Principle: Mirroring makes the other person feel heard and
    compels them to elaborate. It buys you time to think. It
    works because the brain craves completion — a mirror
    creates a gap that must be filled.

    1C. THE ACCUSATION AUDIT (Voss)
    Before they can accuse you of something, accuse yourself
    first. List every negative thing they might think or feel.
    This defuses the negatives before they can weaponise them.

    • "Probabilmente penserai che sono irragionevole..."
      (You'll probably think I'm being unreasonable...)
    • "So che potrebbe sembrare che io stia chiedendo troppo..."
      (I know it might seem like I'm asking too much...)
    • "Potresti pensare che non mi importi della tua
       situazione — non è così"
      (You might think I don't care about your situation
       — that's not the case)
    • "Questo ti sembrerà ingiusto, e capisco perché..."
      (This will seem unfair to you, and I understand why...)
    • "So di non essere nella posizione migliore per chiedere
       questo, ma..."
      (I know I'm not in the best position to ask this, but...)

    Principle: When you say the negative before they do, it
    loses its power. If you say "You'll think I'm greedy" and
    they were thinking it, now they can't say it without seeming
    predictable. Often they'll say "No, non è così" and soften.

    ═══════════════════════════════════════════════════════════════
    PART 2: CALIBRATED QUESTIONS (Voss)
    ═══════════════════════════════════════════════════════════════

    Questions starting with "Come" and "Cosa" give the other
    person the illusion of control while you steer the conversation.
    Avoid "Perché" — it sounds accusatory in Italian.

    • "Come potremmo risolvere questo problema insieme?"
      (How could we solve this problem together?)
    • "Cosa servirebbe per farti sentire più tranquillo/a?"
      (What would it take to make you feel more comfortable?)
    • "Come vorresti procedere?"
      (How would you like to proceed?)
    • "Cosa ti impedisce di accettare questa proposta?"
      (What's preventing you from accepting this proposal?)
    • "Come posso rendere le cose più facili per te?"
      (How can I make things easier for you?)
    • "Cosa succederebbe se non trovassimo un accordo?"
      (What would happen if we didn't reach an agreement?)
    • "Come dovrei interpretare questa risposta?"
      (How should I interpret this response?)
    • "In che modo questo influisce sui tuoi piani?"
      (In what way does this affect your plans?)

    THE POWER OF "COME DOVREI FARE?":
    When someone makes an unreasonable demand, don't say no.
    Ask them how you're supposed to do it.

    • "Come potrei farlo con le risorse che ho?"
      (How could I do that with the resources I have?)
    • "Come sarebbe possibile rispettare questi tempi?"
      (How would it be possible to meet this timeline?)

    Principle: "Come" questions force the other side to solve
    your problem for you. They feel in control. You get solutions.

    ═══════════════════════════════════════════════════════════════
    PART 3: "È VERO" vs "HAI RAGIONE" (Voss)
    ═══════════════════════════════════════════════════════════════

    The two most dangerous words in negotiation are "hai ragione"
    (you're right). When someone says this, they're usually just
    trying to make you stop talking. What you want to hear is
    "è vero" (that's right / that's true) — this means they
    genuinely agree with your summary of their position.

    How to get "è vero":
    1. Listen and absorb their position
    2. Summarise it back to them with a label:
       "Quindi, se ho capito bene, la tua preoccupazione
        principale è... e ti sembra che..."
       (So, if I understand correctly, your main concern is...
        and it seems to you that...)
    3. When they say "Esatto" / "È proprio così" / "È vero" —
       you have broken through.

    • "Fammi vedere se ho capito bene la tua posizione..."
      (Let me see if I've understood your position correctly...)
    • "Se non sbaglio, quello che ti preoccupa di più è..."
      (If I'm not mistaken, what worries you most is...)
    • "Ricapitolando: tu vorresti X, ma temi che Y..."
      (To summarise: you'd like X, but you fear that Y...)

    Principle: People need to feel understood before they will
    move. "È vero" is the signal that you have earned the right
    to propose solutions.

    ═══════════════════════════════════════════════════════════════
    PART 4: THE POWER OF "NO" (Voss)
    ═══════════════════════════════════════════════════════════════

    "Sì" makes people nervous — it feels like commitment.
    "No" makes people feel safe — it feels like control.
    Design your questions so they can say "no" and still
    move toward your goal.

    Instead of: "Ti va bene venerdì?" (Is Friday okay?)
    Ask: "Sarebbe un problema per te se ci vedessimo venerdì?"
      (Would it be a problem for you if we met Friday?)
    → "No, nessun problema" achieves the same result.

    Instead of: "Sei d'accordo?" (Do you agree?)
    Ask: "Hai qualcosa in contrario?" (Do you have any objection?)
    → "No, nessuna obiezione" = agreement without pressure.

    • "C'è qualche motivo per cui non potremmo procedere?"
      (Is there any reason we couldn't proceed?)
    • "Ti dispiacerebbe se provassi un approccio diverso?"
      (Would you mind if I tried a different approach?)
    • "Sarebbe assurdo se ti proponessi...?"
      (Would it be crazy if I proposed...?)
    • "Hai rinunciato a questo progetto?"
      (Have you given up on this project?)
      → A "no" reaffirms their commitment.

    Principle: "No" is not the end of a negotiation. It's the
    beginning. Let people say "no" and they become more open.

    ═══════════════════════════════════════════════════════════════
    PART 5: FAIRNESS AND LOSS AVERSION (Voss + Cialdini)
    ═══════════════════════════════════════════════════════════════

    "Giusto" / "equo" is one of the most powerful words in Italian
    negotiation. People will fight harder to avoid a loss than to
    gain something of equal value.

    5A. USING FAIRNESS

    • "Voglio essere giusto/a con te"
      (I want to be fair with you)
    • "Dimmi se in qualsiasi momento senti che non sono
       giusto/a con te"
      (Tell me if at any point you feel I'm not being fair to you)
    • "Cerchiamo una soluzione che sia equa per entrambi"
      (Let's look for a solution that's fair for both of us)
    • "Non vorrei che pensassi che sto approfittando
       della situazione"
      (I wouldn't want you to think I'm taking advantage
       of the situation)

    5B. FRAMING LOSS vs GAIN

    Weak (gain frame): "Se accetti, risparmierai il 20%"
    Strong (loss frame): "Se non accetti entro venerdì,
    perderai il 20% di sconto"

    • "Ogni giorno che aspettiamo, rischiamo di perdere..."
      (Every day we wait, we risk losing...)
    • "Se non agiamo adesso, potremmo non avere
       un'altra occasione"
      (If we don't act now, we might not get another chance)
    • "Non vorrei che ti sfuggisse questa opportunità"
      (I wouldn't want you to miss this opportunity)

    Principle: The fear of losing something is roughly twice
    as powerful as the pleasure of gaining the same thing.

    ═══════════════════════════════════════════════════════════════
    PART 6: FRAME CONTROL (Klaff)
    ═══════════════════════════════════════════════════════════════

    Whoever sets the frame controls the conversation. A frame
    is the lens through which someone interprets a situation.
    When frames collide, only one survives.

    6A. THE PRIZE FRAME
    Position yourself or your offer as the prize.

    Instead of: "Spero che accettiate la mia proposta"
      (I hope you'll accept my proposal) — supplicant frame
    Use: "Non sono sicuro/a che questo progetto sia adatto
      a tutti — devo capire se siamo compatibili"
      (I'm not sure this project is right for everyone —
       I need to understand if we're compatible)

    • "Lavoriamo solo con chi condivide i nostri valori"
      (We only work with those who share our values)
    • "Prima di procedere, devo capire se questo
       è il progetto giusto per noi"
      (Before proceeding, I need to understand if this
       is the right project for us)
    • "Il mio tempo è limitato, quindi devo scegliere
       con cura i progetti su cui investire"
      (My time is limited, so I need to choose carefully
       which projects to invest in)

    6B. BREAKING THE ANALYST FRAME
    When someone retreats into cold analysis to delay or dismiss,
    use intrigue or novelty to re-engage the emotional brain.

    • "Lascia stare i numeri per un momento — lasciami
       raccontarti cosa è successo quando abbiamo provato
       questo la prima volta"
      (Forget the numbers for a moment — let me tell you
       what happened when we tried this the first time)
    • "I dati li puoi leggere dopo — adesso voglio farti
       capire perché questo è diverso da tutto il resto"
      (You can read the data later — right now I want you
       to understand why this is different from everything else)

    6C. CONTROLLING TIME FRAMES

    • "Devo prendere una decisione entro la settimana"
      (I need to make a decision by the end of the week)
    • "Ho un'altra proposta sul tavolo, ma volevo
       parlarne prima con te"
      (I have another proposal on the table, but I wanted
       to discuss it with you first)
    • "Questa offerta è valida fino a venerdì"
      (This offer is valid until Friday)

    Principle: Status, time, and framing are not about
    arrogance — they are about not being in a position where
    the other person controls all three by default.

    ═══════════════════════════════════════════════════════════════
    PART 7: PUSH-PULL AND CREATING TENSION (Klaff)
    ═══════════════════════════════════════════════════════════════

    Give something positive (pull), then take something away
    or qualify it (push). This creates engagement.

    • "Questa potrebbe essere un'ottima opportunità per te —
       ma non so se i tempi siano giusti dalla tua parte"
      (This could be a great opportunity for you — but I'm
       not sure if the timing is right on your end)
    • "Mi piace molto la tua idea, anche se ho qualche
       dubbio sull'esecuzione"
      (I really like your idea, although I have some doubts
       about the execution)
    • "Potremmo lavorare insieme — dipende da quanto
       sei disposto/a a investire"
      (We could work together — it depends on how much
       you're willing to invest)

    Principle: Constant agreement bores. Constant disagreement
    alienates. The oscillation between the two creates the
    tension that keeps people engaged and attentive.

    ═══════════════════════════════════════════════════════════════
    PART 8: STATUS ALIGNMENT (Klaff)
    ═══════════════════════════════════════════════════════════════

    In any encounter, status is negotiated in the first seconds.
    You don't need to dominate — you need parity.

    8A. ESTABLISHING PARITY

    • "Il mio tempo è prezioso quanto il tuo, quindi
       usiamolo bene"
      (My time is as valuable as yours, so let's use it well)
    • "Sono qui perché credo di poter offrire qualcosa
       di valore — non per chiedere un favore"
      (I'm here because I believe I can offer something
       of value — not to ask for a favour)

    8B. REFUSING TO ACCEPT LOW STATUS

    • "Preferisco che ci parliamo da pari a pari"
      (I'd prefer that we speak as equals)
    • "Non sono venuto/a a supplicare — sono venuto/a
       a proporre qualcosa di reciproco interesse"
      (I didn't come to beg — I came to propose something
       of mutual interest)

    8C. SMALL ACTS OF INDEPENDENCE

    • "Grazie, ma non ho molto tempo — possiamo iniziare?"
      (Thanks, but I don't have much time — shall we start?)

    Principle: Status is not arrogance. It is the refusal to
    enter a conversation from a position of weakness.

    ═══════════════════════════════════════════════════════════════
    PART 9: ANCHORING AND THE ACKERMAN MODEL (Voss)
    ═══════════════════════════════════════════════════════════════

    9A. ANCHORING
    The first number or proposal sets the reference point.

    • "Per un lavoro di questo livello, il compenso standard
       si aggira intorno a..."
      (For work of this level, the standard compensation
       is around...)
    • "Per dare un'idea del valore, progetti simili vengono
       valutati intorno a..."
      (To give an idea of the value, similar projects are
       valued at around...)

    9B. THE ACKERMAN MODEL (for price negotiation)
    Set your target. First offer = 65% of target.
    Increase to 85%, then 95%, then 100% — with decreasing
    increments to signal you're reaching your limit.
    On the final amount, use a non-round number and throw in
    something non-monetary.

    • "Il massimo che posso arrivare è 4.850 — e includo
       anche la consulenza iniziale"
      (The most I can go is 4,850 — and I'll include
       the initial consultation as well)

    Principle: Non-round numbers (4.850 not 5.000) signal
    that you've calculated carefully and reached a real limit.

    ═══════════════════════════════════════════════════════════════
    PART 10: THE RULE OF THREE (Voss)
    ═══════════════════════════════════════════════════════════════

    Get the other person to agree to something three times in
    the same conversation. Each confirmation reduces the chance
    they're lying or will back out later.

    1st: Direct agreement: "Quindi siamo d'accordo su X?"
    2nd: Label + confirm: "Mi sembra che X sia importante
         per te — è così?"
    3rd: Calibrated question: "Come vorresti procedere con X?"

    If their answer changes or becomes vague by the third time,
    they may not be genuinely committed.

    Principle: Liars and uncommitted people have difficulty
    maintaining consistency across three different framings
    of the same commitment.

    ═══════════════════════════════════════════════════════════════
    PART 11: ADVANCED CARNEGIE IN NEGOTIATION
    ═══════════════════════════════════════════════════════════════

    11A. LETTING THE IDEA BE THEIRS

    • "Tu cosa proporresti?" (What would you propose?)
    • "Come la risolveresti tu questa situazione?"
      (How would you solve this situation?)
    • "Partendo dalla tua idea di prima, potremmo..."
      (Starting from your earlier idea, we could...)

    11B. BEGINNING WITH COMMON GROUND

    • "Su questo punto siamo già d'accordo, giusto?"
      (We already agree on this point, right?)
    • "Entrambi vogliamo la stessa cosa —
       troviamo il modo migliore per arrivarci"
      (We both want the same thing — let's find
       the best way to get there)

    11C. DRAMATISING YOUR IDEAS

    • "Immagina di essere un cliente che apre questa pagina
       per la prima volta..."
      (Imagine being a customer who opens this page
       for the first time...)
    • "Ti faccio un esempio concreto di quello
       che intendo..."
      (Let me give you a concrete example of what I mean...)
    • "Lascia che ti racconti cosa è successo a un mio
       collega nella stessa situazione..."
      (Let me tell you what happened to a colleague of mine
       in the same situation...)

    ═══════════════════════════════════════════════════════════════
    PART 12: LATE-NIGHT FM DJ VOICE (Voss)
    ═══════════════════════════════════════════════════════════════

    Tone of voice matters more than words. Three voices:
    1. Assertive (use rarely): direct, confident
    2. Playful/positive (default): smile in your voice
    3. Late-night FM DJ (for difficult moments): slow, calm,
       downward-inflecting, soothing

    In Italian, this voice uses:
    • Longer sentences with subjunctive softening
    • Conditional tense for politeness
    • Low, calm register
    • Strategic pauses

    "Capisco... e mi chiedo se potremmo trovare un modo
     per andare incontro a entrambi..." (slow, calm, low)

    Principle: When tension rises, slow down. Lower your voice.
    Speak as if you have all the time in the world.

    ═══════════════════════════════════════════════════════════════
    PART 13: BLACK SWANS (Voss)
    ═══════════════════════════════════════════════════════════════

    Black Swans are the unknown unknowns — the hidden pieces
    of information that, if discovered, completely change
    the negotiation. Every negotiation has at least three.

    Questions to uncover Black Swans:

    • "C'è qualcosa che non sto considerando?"
      (Is there something I'm not considering?)
    • "Cosa ti ha portato a questa posizione?"
      (What led you to this position?)
    • "Cosa cambierebbe tutto per te?"
      (What would change everything for you?)
    • "C'è qualcun altro coinvolto nella decisione?"
      (Is there someone else involved in the decision?)
    • "Cosa succederebbe se non raggiungessimo un accordo?"
      (What would happen if we didn't reach an agreement?)

    Principle: The person with the most information wins.
    Never assume you know everything. Keep asking.
    """,

    "Riconoscere e rispondere a manipolazioni": """
    Recognising and responding to manipulation tactics — from
    everyday social pressure to strategic deception. Draws on
    Cialdini's influence principles (as warning signs), Machiavelli's
    insights on power (as awareness tools), and practical Italian
    phrases for calling out or deflecting each tactic.

    ═══════════════════════════════════════════════════════════════
    PART 1: RECIPROCITY TRAPS (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: We feel obligated to return favours — even ones
    we never asked for. Manipulators exploit this by giving
    unsolicited gifts, favours, or concessions to create debt.

    RECOGNISING:
    • Unsolicited favours followed by requests
    • "After all I've done for you..."
    • Gifts with strings attached
    • Concessions designed to make you concede in return

    Red flag phrases (what they say):
    • "Ti ho aiutato io quando ne avevi bisogno..."
      (I helped you when you needed it...)
    • "Dopo tutto quello che ho fatto per te..."
      (After everything I've done for you...)
    • "Ti ho dato X — è il minimo che tu possa fare"
      (I gave you X — it's the least you can do)

    RESPONDING:
    • "Apprezzo il gesto, ma questo non cambia
       la mia decisione su quest'altra questione"
      (I appreciate the gesture, but it doesn't change
       my decision on this other matter)
    • "Sono grato/a per quello che hai fatto. Tuttavia,
       sono due cose separate"
      (I'm grateful for what you did. However, these are
       two separate things)
    • "Non ricordo di aver chiesto quel favore —
       e non credo che mi obblighi a nulla"
      (I don't recall asking for that favour — and I don't
       believe it obligates me to anything)
    • "Preferisco valutare questa decisione
       indipendentemente dai favori passati"
      (I prefer to evaluate this decision independently
       of past favours)

    ═══════════════════════════════════════════════════════════════
    PART 2: COMMITMENT AND CONSISTENCY TRAPS (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: Once we commit to something — even something
    small — we feel pressure to remain consistent with that
    commitment, even when it no longer makes sense.

    Techniques to watch for:
    • "Foot in the door": small request first, then escalation
    • "Lowball": change terms after commitment
    • Public commitments used to trap you later
    • "You said you would..."

    Red flag phrases:
    • "Ma tu avevi detto che..."
      (But you said that...)
    • "Avevi già accettato — non puoi tornare indietro adesso"
      (You already agreed — you can't go back now)
    • "Sei una persona coerente, vero? Allora..."
      (You're a consistent person, right? So...)
    • "Hai già investito troppo per smettere adesso"
      (You've already invested too much to stop now)

    RESPONDING:
    • "È vero che ho detto/fatto X. Ma le circostanze sono
       cambiate, e ho il diritto di rivalutare"
      (It's true I said/did X. But circumstances have changed,
       and I have the right to reassess)
    • "La coerenza per me significa fare la cosa giusta,
       non restare agganciato/a a una decisione superata"
      (Consistency for me means doing the right thing,
       not clinging to an outdated decision)
    • "Preferisco cambiare idea quando ho informazioni
       nuove piuttosto che insistere per orgoglio"
      (I'd rather change my mind when I have new information
       than persist out of pride)
    • "Il fatto che abbia investito tempo/denaro non è un motivo
       per investirne ancora di più in qualcosa che non funziona"
      (The fact that I invested time/money isn't a reason to
       invest even more in something that isn't working)
      — This is the sunk cost fallacy: name it.

    ═══════════════════════════════════════════════════════════════
    PART 3: SOCIAL PROOF PRESSURE (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: We look to others to determine correct behaviour.
    Manipulators fabricate or exaggerate consensus to pressure you.

    Red flag phrases:
    • "Lo fanno tutti" (Everyone does it)
    • "Tutti i tuoi colleghi sono d'accordo" (All your colleagues agree)
    • "Nessuno ha mai avuto problemi con questo"
      (Nobody has ever had problems with this)
    • "Il 95% dei nostri clienti sceglie questo piano"
      (95% of our clients choose this plan)
    • "Sarai l'unico/a a non partecipare"
      (You'll be the only one not participating)

    RESPONDING:
    • "Il fatto che altri lo facciano non significa
       che sia la scelta giusta per me"
      (The fact that others do it doesn't mean it's
       the right choice for me)
    • "Preferisco decidere in base alla mia situazione,
       non in base a quello che fanno gli altri"
      (I prefer to decide based on my situation,
       not based on what others do)
    • "Posso avere dei dati concreti invece che
       affermazioni generiche?"
      (Can I have concrete data instead of generic claims?)
    • "Anche se fosse vero, ogni situazione è diversa"
      (Even if that were true, every situation is different)
    • "'Tutti' chi, esattamente?"
      ("Everyone" who, exactly?)

    ═══════════════════════════════════════════════════════════════
    PART 4: AUTHORITY EXPLOITATION (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: We defer to authority — titles, uniforms, expertise.
    Manipulators invoke authority (real or fake) to bypass
    your critical thinking.

    Red flag phrases:
    • "Gli esperti dicono che..." (senza specificare quali)
      (Experts say that... — without specifying which)
    • "Secondo uno studio..." (senza citazione)
      (According to a study... — without citation)
    • "Fidati, ho vent'anni di esperienza"
      (Trust me, I have twenty years of experience)
    • "Il mio avvocato/commercialista dice che..."
      (My lawyer/accountant says that...)
    • "È la legge" / "Sono le regole"
      (It's the law / Those are the rules)

    RESPONDING:
    • "Interessante — quali esperti esattamente?
       Mi piacerebbe approfondire"
      (Interesting — which experts exactly?
       I'd like to look into it further)
    • "L'esperienza è importante, ma i fatti
       lo sono di più — posso vedere i dati?"
      (Experience is important, but facts are more
       so — can I see the data?)
    • "Posso verificare personalmente?"
      (Can I verify this myself?)
    • "Con tutto il rispetto per la sua esperienza,
       ho bisogno di capire con i miei occhi"
      (With all due respect for your experience,
       I need to understand with my own eyes)
    • "Quale legge esattamente? Mi può indicare
       l'articolo?"
      (Which law exactly? Can you point me to
       the specific article?)

    ═══════════════════════════════════════════════════════════════
    PART 5: FALSE SCARCITY AND URGENCY (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: We want what is scarce. Manipulators create
    artificial time pressure or limited availability to prevent
    careful thought.

    Red flag phrases:
    • "È l'ultima occasione" (It's the last chance)
    • "L'offerta scade oggi" (The offer expires today)
    • "Ne restano solo due" (There are only two left)
    • "Se non decidi adesso, perdi tutto"
      (If you don't decide now, you lose everything)
    • "C'è un altro acquirente interessato"
      (There's another interested buyer)
    • "Devi decidere subito" (You have to decide immediately)

    RESPONDING:
    • "Se l'offerta è valida oggi, sarà valida anche domani.
       Se non lo è, allora non era un'offerta seria"
      (If the offer is valid today, it'll be valid tomorrow too.
       If it isn't, then it wasn't a serious offer)
    • "Non prendo decisioni importanti sotto pressione"
      (I don't make important decisions under pressure)
    • "Mi serve tempo per riflettere. Se questo è un problema,
       probabilmente non è l'affare giusto per me"
      (I need time to reflect. If that's a problem,
       it's probably not the right deal for me)
    • "La fretta è la peggior consigliera"
      (Haste is the worst adviser — Italian proverb)
    • "Se devo decidere adesso, la risposta è no.
       Se posso pensarci, potrebbe essere sì"
      (If I have to decide now, the answer is no.
       If I can think about it, it could be yes)

    ═══════════════════════════════════════════════════════════════
    PART 6: LIKING EXPLOITATION (Cialdini)
    ═══════════════════════════════════════════════════════════════

    Principle: We say yes more easily to people we like.
    Manipulators manufacture rapport through: flattery,
    similarity, physical attractiveness, association with
    positive things, and compliments.

    Red flag phrases:
    • "Noi siamo uguali, tu e io" (We're the same, you and I)
    • "Solo a te faccio questo prezzo" (Only for you this price)
    • "Lo faccio perché mi stai simpatico/a"
      (I'm doing it because I like you)
    • Excessive compliments before a request
    • Sudden warmth from someone who wants something

    RESPONDING:
    • "Apprezzo le parole gentili. Adesso parliamo dei fatti"
      (I appreciate the kind words. Now let's talk about facts)
    • "Grazie, ma preferisco valutare la proposta
       indipendentemente dal rapporto personale"
      (Thanks, but I prefer to evaluate the proposal
       independently of our personal relationship)
    • "Mi fa piacere che andiamo d'accordo, ma la mia
       decisione si basa su altri criteri"
      (I'm glad we get along, but my decision is based
       on other criteria)
    • Internally: "Mi piace questa persona. La mia simpatia
       sta influenzando il mio giudizio?"
      (I like this person. Is my liking them
       affecting my judgment?)

    ═══════════════════════════════════════════════════════════════
    PART 7: MACHIAVELLIAN TACTICS — RECOGNITION AND DEFENCE
    ═══════════════════════════════════════════════════════════════

    Machiavelli wrote The Prince as a manual for rulers, but its
    insights apply to recognising power dynamics in any context.
    The goal is awareness, not imitation.

    7A. MANAGING APPEARANCES (Machiavelli: appear merciful,
    faithful, humane, upright, religious — but be prepared
    to act otherwise)

    People may project qualities they don't possess. Judge by
    actions over time, not by declarations or first impressions.

    • "Le parole sono belle, ma i fatti parlano più chiaro"
      (The words are nice, but actions speak louder)
    • "Mi fido di quello che vedo, non di quello che sento"
      (I trust what I see, not what I hear)
    • "Preferirei giudicare dai risultati, non dalle promesse"
      (I'd prefer to judge by results, not promises)
    • "C'è una differenza tra quello che dice e quello che fa"
      (There's a difference between what they say and what they do)

    7B. THE FOX AND THE LION (Machiavelli: one must be a fox
    to recognise traps, and a lion to frighten wolves)

    Recognising when someone uses cunning (fox) vs brute force (lion):

    Fox tactics (cunning):
    • Changing the subject when caught
    • Redefining words to avoid accountability
    • Using ambiguity so they can deny later
    • Plausible deniability: "Non ho mai detto esattamente questo"
      (I never said exactly that)

    • "Stai cambiando le carte in tavola"
      (You're changing the cards on the table — reshuffling)
    • "Non è quello che avevamo concordato —
       ho la comunicazione scritta"
      (That's not what we agreed — I have it in writing)
    • "Preferirei che fossimo precisi nelle parole"
      (I'd prefer that we be precise with our words)
    • "Potresti ripeterlo per iscritto?"
      (Could you repeat that in writing?)

    Lion tactics (intimidation):
    • Raising voice, threatening consequences
    • Using position/power to force compliance
    • "You'll regret this" style threats

    • "Abbassare la voce non rende il tuo argomento
       meno valido — e alzarla non lo rende più valido"
      (Lowering your voice doesn't make your argument less
       valid — and raising it doesn't make it more valid)
    • "Le minacce non mi aiutano a prendere una decisione
       migliore — parliamo nel merito"
      (Threats don't help me make a better decision —
       let's discuss the substance)
    • "Non rispondo a pressioni. Rispondo ad argomenti"
      (I don't respond to pressure. I respond to arguments)

    7C. ENDS JUSTIFYING MEANS

    • "L'obiettivo può essere giusto, ma il metodo
       non mi convince"
      (The goal may be right, but the method
       doesn't convince me)
    • "Non sono d'accordo che il fine giustifichi qualsiasi mezzo"
      (I don't agree that the end justifies any means)
    • "Come ci arriviamo è importante quanto dove arriviamo"
      (How we get there is as important as where we get)

    7D. DIVIDE AND CONQUER

    • "Mi hai detto una cosa e a lui ne hai detta un'altra —
       possiamo chiarire insieme?"
      (You told me one thing and told him another —
       can we clarify together?)
    • "Preferisco che ne parliamo tutti insieme"
      (I'd prefer we discuss this all together)
    • "Prima di decidere, vorrei sentire anche l'altra versione"
      (Before deciding, I'd like to hear the other side too)

    7E. FLATTERY AS A TOOL

    • "I complimenti mi fanno piacere, ma non influenzano
       le mie decisioni"
      (Compliments please me, but they don't influence
       my decisions)
    • "Preferisco la verità scomoda alla bugia confortevole"
      (I prefer the uncomfortable truth to the comfortable lie)
    • "Dimmi quello che pensi davvero, non quello
       che vuoi che io senta"
      (Tell me what you really think, not what
       you want me to hear)

    7F. FORTUNE FAVOURS THE BOLD — RECOGNISING WHEN SOMEONE
    EXPLOITS THIS AGAINST YOU

    • "La prudenza non è debolezza — è saggezza"
      (Caution is not weakness — it is wisdom)
    • "Preferisco perdere un'opportunità che prendere
       una decisione affrettata"
      (I'd rather miss an opportunity than make
       a hasty decision)

    ═══════════════════════════════════════════════════════════════
    PART 8: GASLIGHTING AND REALITY DISTORTION
    ═══════════════════════════════════════════════════════════════

    When someone denies your experience, memory, or perception
    to make you doubt yourself.

    Red flag phrases:
    • "Non è mai successo" (That never happened)
    • "Te lo sei inventato/a" (You made that up)
    • "Sei troppo sensibile" (You're too sensitive)
    • "Non ho mai detto questo" (I never said that)
    • "Stai esagerando" (You're exaggerating)
    • "Il problema sei tu" (The problem is you)

    RESPONDING:
    • "Io ricordo le cose diversamente, e la mia
       memoria è valida quanto la tua"
      (I remember things differently, and my memory
       is as valid as yours)
    • "Non mettere in discussione la mia percezione —
       ho vissuto questa esperienza"
      (Don't question my perception —
       I lived through this experience)
    • "Ho le prove di quello che dico"
      (I have proof of what I'm saying)
    • "Il fatto che tu non la veda così non significa
       che non sia successo"
      (The fact that you don't see it that way doesn't mean
       it didn't happen)
    • "Possiamo verificare — controlliamo le email/i messaggi"
      (We can check — let's look at the emails/messages)

    Principle: Trust your own perception. Document important
    communications in writing. When someone consistently
    denies reality, the pattern itself is the evidence.

    ═══════════════════════════════════════════════════════════════
    PART 9: EMOTIONAL MANIPULATION PATTERNS
    ═══════════════════════════════════════════════════════════════

    9A. GUILT-TRIPPING

    • "Non mi fare sentire in colpa per una decisione
       che è nel mio diritto prendere"
      (Don't make me feel guilty for a decision
       that is within my rights to make)
    • "Capisco che tu sia deluso/a, ma questo non cambia
       quello che è giusto per me"
      (I understand you're disappointed, but that doesn't
       change what's right for me)

    9B. PLAYING THE VICTIM

    • "Capisco che stai soffrendo, ma anche io ho
       delle esigenze"
      (I understand you're suffering, but I also
       have my own needs)
    • "Il tuo dolore è reale, ma non può essere usato
       per controllare le mie scelte"
      (Your pain is real, but it can't be used
       to control my choices)

    9C. SILENT TREATMENT / WITHHOLDING

    • "Se hai un problema, preferisco che me lo dica
       direttamente"
      (If you have a problem, I'd prefer you tell me directly)
    • "Il silenzio non risolve nulla — parliamo"
      (Silence doesn't solve anything — let's talk)

    9D. MOVING THE GOALPOSTS

    • "Avevamo concordato X, e io ho fatto la mia parte"
      (We agreed on X, and I did my part)
    • "I criteri non possono cambiare dopo
       che ho soddisfatto quelli originali"
      (The criteria can't change after I've met
       the original ones)
    • "Noto che le condizioni continuano a cambiare —
       possiamo fissarle per iscritto?"
      (I notice the conditions keep changing —
       can we put them in writing?)

    ═══════════════════════════════════════════════════════════════
    PART 10: META-AWARENESS AND SELF-DEFENCE PRINCIPLES
    ═══════════════════════════════════════════════════════════════

    General defensive principles for any manipulation:

    10A. SLOW DOWN

    • "Devo pensarci" (I need to think about it)
    • "Mi serve un po' di tempo prima di rispondere"
      (I need some time before responding)
    • "Non rispondo a caldo — ne parliamo domani"
      (I don't respond in the heat of the moment —
       let's talk about it tomorrow)

    10B. GET IT IN WRITING

    • "Potresti mandarmi un riepilogo via email?"
      (Could you send me a summary by email?)
    • "Per chiarezza, riepilogo per iscritto
       quello che abbiamo concordato"
      (For clarity, I'll summarise in writing
       what we've agreed on)

    10C. INVOLVE WITNESSES OR THIRD PARTIES

    • "Preferirei che questa conversazione avvenisse
       in presenza di tutti gli interessati"
      (I'd prefer this conversation to take place
       in the presence of all stakeholders)

    10D. TRUST YOUR GUT

    • "Qualcosa non mi torna in questa situazione"
      (Something doesn't add up in this situation)
    • "Ho bisogno di capire meglio prima di procedere"
      (I need to understand better before proceeding)
    • "Non mi sento a mio agio con questa proposta,
       e questo è un segnale importante"
      (I don't feel comfortable with this proposal,
       and that's an important signal)

    10E. NAME THE PATTERN

    • "Mi sembra che tu stia cercando di mettermi fretta"
      (It seems to me you're trying to rush me)
    • "Noto che ogni volta che sollevo questo punto,
       il discorso cambia"
      (I notice that every time I raise this point,
       the subject changes)
    • "Questo mi sembra un tentativo di farmi sentire
       in colpa per influenzare la mia decisione"
      (This feels like an attempt to make me feel guilty
       to influence my decision)
    • "Stai usando quello che ho fatto prima per
       condizionare quello che faccio adesso?"
      (Are you using what I did before to condition
       what I do now?)

    Principle: Once you name a tactic out loud, it loses
    most of its power. Manipulators rely on invisibility.
    """,
}

# --------------------------------
# Topic resources
# --------------------------------
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

# Topics whose TOPIC_NUANCES have been substantially expanded and need
# new questions even if they already meet MIN_PER_TOPIC.  Maps topic name
# to a higher per-topic minimum.  Remove entries once caught up.
REFRESHED_TOPICS: Dict[str, int] = {
    "Negoziazione e persuasione": 50,
    "Riconoscere e rispondere a manipolazioni": 50,
    "Gestione delle conversazioni difficili": 40,
    "Conflitto e risoluzione (avanzato)": 40,
}

DIFFICULT_TOPICS = {
    "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)",
    "Passato prossimo (reg/irr; essere/avere; accordo)",
    "Imperfetto vs. Passato Prossimo",
    "Pronomi Combinati",
    "Pronomi Relativi",
    "Si Impersonale",
    "Volerci vs. Metterci",
    "Imperativo Formale",
    "Preposizioni articolate",
    "Forme negative composte (non...mai, non...niente, ecc.)",
    "Plurali irregolari (nomi con plurale irregolare)",
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
    "Comunicazione efficace e influenza sociale",
    "Concordanza dei tempi (casi tipici)",
    "Periodo ipotetico II e III",
    "Congiuntivo passato",
    "Congiuntivo imperfetto",
    "Congiuntivo trapassato",
    "Passato remoto (introduzione)",
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
# State
# --------------------------------

def load_state() -> Dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"coverage": {}, "seen_questions": {}}

def save_state(state: Dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# --------------------------------
# CSV stats (ground truth)
# --------------------------------

def load_csv_stats(level: str) -> Tuple[Set[str], Counter]:
    """Read the CSV and return (seen question_texts, topic counts).

    Topic names are matched case-insensitively to the defined topic list so
    that questions generated by older script versions (with different
    capitalisation or punctuation) are still counted correctly.
    """
    OUTDIR.mkdir(parents=True, exist_ok=True)
    path = OUTDIR / f"Italian_{level}.csv"
    seen: Set[str] = set()
    topic_counts: Counter = Counter()

    if not path.exists():
        return seen, topic_counts

    # Build a lowercase → canonical name lookup for fast matching
    defined_topics = TOPICS_BY_LEVEL[level]
    canonical: Dict[str, str] = {t.lower(): t for t in defined_topics}

    unrecognised: Counter = Counter()

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qt = (row.get("question_text") or "").strip()
            topic = (row.get("topic") or "").strip()
            if qt:
                seen.add(qt)
            if topic:
                matched = canonical.get(topic.lower())
                if matched:
                    topic_counts[matched] += 1
                else:
                    unrecognised[topic] += 1

    if unrecognised:
        total_unrecognised = sum(unrecognised.values())
        print(f"  ⚠️  {total_unrecognised} questions have unrecognised topic names "
              f"({len(unrecognised)} distinct). They won't count toward coverage.")
        print(f"     Top unrecognised: " +
              ", ".join(f'"{t}" ({c})' for t, c in unrecognised.most_common(3)))

    return seen, topic_counts

# --------------------------------
# Topic planning
# --------------------------------

def plan_topics(
    level: str,
    state: Dict,
    batch_size: int,
    topic_counts: Counter,
) -> Tuple[List[str], Dict[str, int]]:
    """Deterministic slots for below-minimum topics, weighted random for the rest."""
    topics = TOPICS_BY_LEVEL[level]

    cov = state["coverage"].setdefault(level, {})
    for t in topics:
        cov.setdefault(t, 0)

    # Per-topic minimum: use REFRESHED_TOPICS override if present, else MIN_PER_TOPIC
    def topic_min(t: str) -> int:
        return REFRESHED_TOPICS.get(t, MIN_PER_TOPIC)

    # 1. Guaranteed slots for topics below their minimum
    below_min = sorted(
        [t for t in topics if topic_counts.get(t, 0) < topic_min(t)],
        key=lambda t: topic_counts.get(t, 0),
    )

    picks: List[str] = []
    slot_counts: Dict[str, int] = {}
    idx = 0
    while len(picks) < batch_size and below_min:
        t = below_min[idx % len(below_min)]
        if slot_counts.get(t, 0) < 3:
            picks.append(t)
            slot_counts[t] = slot_counts.get(t, 0) + 1
        idx += 1
        if idx > len(below_min) * 3 * batch_size:
            break

    # 2. Weighted random for remaining space
    remaining = batch_size - len(picks)
    if remaining > 0:
        weights = []
        for t in topics:
            csv_count = topic_counts.get(t, 0)
            t_min = topic_min(t)
            shortfall = max(0, t_min - csv_count)
            base_weight = (1.0 + shortfall) if shortfall > 0 else 1.0 / (1.0 + csv_count - t_min + 1)
            if t in DIFFICULT_TOPICS:
                base_weight *= 1.5
            weights.append(base_weight)

        filler_counts = {t: picks.count(t) for t in topics}
        max_per_topic = max(3, batch_size // 3)

        for _ in range(remaining):
            eligible = [(t, w) for t, w in zip(topics, weights) if filler_counts.get(t, 0) < max_per_topic]
            if not eligible:
                eligible = list(zip(topics, weights))
            chosen_topics, chosen_weights = zip(*eligible)
            choice = random.choices(chosen_topics, weights=chosen_weights, k=1)[0]
            picks.append(choice)
            filler_counts[choice] = filler_counts.get(choice, 0) + 1

    # 3. Bonus for difficult topics
    difficult_count = sum(1 for t in picks if t in DIFFICULT_TOPICS)
    bonus_allowed = min(BONUS_MAX, int(difficult_count * 0.5))
    if bonus_allowed > 0:
        difficult_in_batch = [t for t in picks if t in DIFFICULT_TOPICS]
        for _ in range(bonus_allowed):
            if difficult_in_batch:
                picks.append(random.choice(difficult_in_batch))

    plan_counts: Dict[str, int] = {}
    for t in picks:
        plan_counts[t] = plan_counts.get(t, 0) + 1

    return picks, plan_counts

# --------------------------------
# Coverage stats
# --------------------------------

def get_coverage_stats(level: str, topic_counts: Counter) -> str:
    topics = TOPICS_BY_LEVEL[level]
    total = sum(topic_counts.values())
    sorted_topics = sorted(topics, key=lambda t: topic_counts.get(t, 0))

    def topic_min(t: str) -> int:
        return REFRESHED_TOPICS.get(t, MIN_PER_TOPIC)

    zero_topics  = [t for t in sorted_topics if topic_counts.get(t, 0) == 0]
    below_min    = [t for t in sorted_topics if 0 < topic_counts.get(t, 0) < topic_min(t)]
    at_min       = [t for t in sorted_topics if topic_counts.get(t, 0) >= topic_min(t)]

    lines = [
        f"Coverage for {level}  ({len(topics)} topics | {total} total questions)",
        f"  Minimum target : {MIN_PER_TOPIC} questions per topic",
        f"  Avg per topic  : {total / len(topics):.1f}" if topics else "",
        f"  At/above min   : {len(at_min)}/{len(topics)} topics ✅",
        f"  Needs work     : {len(zero_topics) + len(below_min)} topics",
    ]

    if zero_topics:
        lines.append(f"  ── 🔴 ZERO questions ({len(zero_topics)} topics) ──")
        for t in zero_topics[:10]:
            marker = " [DIFFICULT]" if t in DIFFICULT_TOPICS else ""
            lines.append(f"    • {t}{marker}")
        if len(zero_topics) > 10:
            lines.append(f"    … and {len(zero_topics) - 10} more")

    if below_min:
        lines.append(f"  ── 🟡 Below minimum ({len(below_min)} topics) ──")
        for t in below_min[:10]:
            count = topic_counts.get(t, 0)
            t_min = topic_min(t)
            shortfall = t_min - count
            marker = " [DIFFICULT]" if t in DIFFICULT_TOPICS else ""
            refresh = " [REFRESHED]" if t in REFRESHED_TOPICS else ""
            lines.append(f"    • {t}: {count}/{t_min} (needs {shortfall} more){marker}{refresh}")
        if len(below_min) > 10:
            lines.append(f"    … and {len(below_min) - 10} more")

    return "\n".join(lines)

# --------------------------------
# Sample previous questions
# --------------------------------

def sample_previous_questions(level: str, max_chars: int = 2000) -> List[str]:
    seen, _ = load_csv_stats(level)
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
        if total + len(q) + 3 > max_chars:
            break
        out.append(q)
        total += len(q) + 3
    return out[:15]

# --------------------------------
# Resource URL
# --------------------------------

def get_resource_url(topic: str) -> str:
    if topic in TOPIC_RESOURCES:
        return TOPIC_RESOURCES[topic]
    for key, url in TOPIC_RESOURCES.items():
        if key.lower() in topic.lower() or topic.lower() in key.lower():
            return url
    return "https://www.lawlessitalian.com/grammar/"

# --------------------------------
# Build prompt
# --------------------------------

def build_claude_prompt(
    level: str,
    topics_plan: List[str],
    batch_size: int,
    state: Dict,
    topic_counts: Counter,
) -> str:
    plan_counts: Dict[str, int] = {}
    for t in topics_plan:
        plan_counts[t] = plan_counts.get(t, 0) + 1

    # Priority block
    priority_lines = []
    for topic, count in sorted(plan_counts.items(), key=lambda x: topic_counts.get(x[0], 0)):
        csv_count = topic_counts.get(topic, 0)
        t_min = REFRESHED_TOPICS.get(topic, MIN_PER_TOPIC)
        shortfall = max(0, t_min - csv_count)
        if csv_count == 0:
            urgency = f"🔴 MISSING — needs {t_min} questions"
        elif shortfall > 0:
            urgency = f"🟡 LOW — {csv_count}/{t_min} questions (needs {shortfall} more)"
        else:
            urgency = f"✅ {csv_count} questions (minimum met)"
        diff_marker = " [difficult topic — extra care]" if topic in DIFFICULT_TOPICS else ""
        priority_lines.append(f"  - {topic}: need {count} more  |  {urgency}{diff_marker}")
    priority_block = "\n".join(priority_lines)

    prior_sample = sample_previous_questions(level)
    prior_block = "\n".join(f"- {q}" for q in prior_sample) if prior_sample else "(none available)"

    total_questions = len(topics_plan)
    write_count = max(1, int(total_questions * WRITE_RATIO)) if total_questions >= 10 else 0
    fill_count = total_questions - write_count

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

    topic_instructions = []
    for topic in set(topics_plan):
        if topic in TOPIC_NUANCES:
            topic_instructions.append(f"\n### {topic}\n{TOPIC_NUANCES[topic]}")
    topic_notes_section = (
        "\n## TOPIC-SPECIFIC REQUIREMENTS\n" + "\n".join(topic_instructions)
        if topic_instructions else ""
    )

    prompt = f"""You are an expert Italian language educator creating questions for CEFR level {level}.

## CRITICAL REQUIREMENTS

Generate EXACTLY {total_questions} questions with perfect accuracy. Quality over quantity.

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

## ⚡ PRIORITY TOPICS FOR THIS BATCH

Topics are ordered by urgency. Generate questions for 🔴 and 🟡 topics first.

{priority_block}

## HINT PHILOSOPHY

Hints are ONLY for disambiguation when:
1. Multiple grammatically correct answers would change meaning/agreement
2. Similar words could be confused (e.g., tè vs tisana)
3. Context alone doesn't clarify gender/number/formality

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
- All options should attempt the translation (even if wrong)

## AVOID THESE EXISTING QUESTIONS

Do not duplicate or closely resemble:
{prior_block}

## QUALITY CHECKLIST

✓ Complete sentence is natural, idiomatic Italian
✓ English translation is perfectly accurate (including plural markers)
✓ Hint is minimal and only if essential
✓ Resource URL is specific to the topic when available
✓ Explanation cites the specific grammar rule
✓ Level {level} appropriate vocabulary and grammar

Generate the {total_questions} questions now. Return ONLY valid JSON."""

    return prompt

# --------------------------------
# Validation
# --------------------------------

def validate_question(item: dict, level: str, index: int) -> List[str]:
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in item or item[field] is None:
            errors.append(f"Missing field: {field}")
            return errors
        if isinstance(item[field], str):
            item[field] = item[field].strip()

    qt = item["question_text"]
    cs = item["complete_sentence"]
    is_write = qt.startswith("Write: ")

    if is_write:
        if "___" in qt:
            errors.append("Write: questions must not contain ___")
        if not qt[7:].strip():
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

    options = [item[f"option_{x}"] for x in ["a", "b", "c", "d"]]
    if len(set(options)) < 4:
        errors.append("Duplicate options found")
    if any(not opt for opt in options):
        errors.append("Empty option found")
    if item["cefr_level"] != level:
        errors.append(f"Wrong level: {item['cefr_level']} (expected {level})")
    if item["topic"] not in TOPICS_BY_LEVEL[level]:
        errors.append(f"Invalid topic: {item['topic']}")

    eng = item["english_translation"].lower()
    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for day in weekdays:
        if f"on {day}" in eng and cs.startswith("Il ") and day + "s" not in eng:
            errors.append(f"Habitual weekday should be 'On {day}s' not 'On {day}'")

    if not is_write and item["hint"]:
        hint_lower = item["hint"].lower()
        for opt in options:
            if hint_lower == opt.lower():
                errors.append("Hint reveals an answer option")

    return errors

def validate_and_clean(batch: List[dict], level: str) -> Tuple[List[QA], List[str]]:
    all_errors = []
    cleaned: List[QA] = []
    for i, item in enumerate(batch, 1):
        errors = validate_question(item, level, i)
        if errors:
            all_errors.append(f"Question {i}: " + "; ".join(errors))
            continue
        for field in REQUIRED_FIELDS:
            if isinstance(item[field], str):
                item[field] = item[field].strip()
            elif item[field] is None:
                item[field] = ""
        item["correct_option"] = item["correct_option"].upper()
        if item["resource"] == "https://www.lawlessitalian.com/grammar/":
            item["resource"] = get_resource_url(item["topic"])
        cleaned.append(QA(**{k: item[k] for k in REQUIRED_FIELDS}))
    return cleaned, all_errors

# --------------------------------
# CSV output
# --------------------------------

def append_csv(level: str, items: List[QA]) -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    path = OUTDIR / f"Italian_{level}.csv"
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

# --------------------------------
# UI helpers
# --------------------------------

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

# --------------------------------
# Main workflows
# --------------------------------

def manual_bridge_claude(levels: List[str], target: int, batch_size: int):
    state = load_state()

    for level in levels:
        seen, topic_counts = load_csv_stats(level)
        done = len(seen)

        clear_screen()
        print_separator("=")
        print(f"  ITALIAN MCQ GENERATOR - Level {level}")
        print(f"  Progress: {done}/{target} questions")
        print(f"  Topics: {len(TOPICS_BY_LEVEL[level])}")
        print_separator("=")

        while done < target:
            # Refresh from CSV each iteration
            seen, topic_counts = load_csv_stats(level)
            done = len(seen)

            remaining = target - done
            current_batch = min(batch_size, remaining)

            print("\n" + get_coverage_stats(level, topic_counts))
            print(f"\n📝 Generating batch of {current_batch} questions...")

            topics, plan_counts = plan_topics(level, state, current_batch, topic_counts)
            print(f"Topics for this batch: {', '.join(set(topics))}")

            prompt = build_claude_prompt(level, topics, current_batch, state, topic_counts)

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

            try:
                if "```json" in raw:
                    raw = raw.split("```json")[1].split("```")[0]
                elif "```" in raw:
                    raw = raw.split("```")[1].split("```")[0]
                payload = json.loads(raw)
                if not isinstance(payload, dict) or "questions" not in payload:
                    raise ValueError("Response must be a JSON object with 'questions' array")
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON: {e}")
                input("\nPress Enter to retry this batch...")
                continue
            except ValueError as e:
                print(f"❌ Format error: {e}")
                input("\nPress Enter to retry this batch...")
                continue

            raw_items = payload.get("questions", [])
            cleaned, errs = validate_and_clean(raw_items, level)

            if errs:
                print(f"\n⚠️  Validation issues found ({len(errs)} errors):")
                for e in errs[:5]:
                    print(f"  • {e}")
                if len(errs) > 5:
                    print(f"  ... and {len(errs) - 5} more")
                if len(cleaned) == 0:
                    print("\n❌ All questions failed validation. Please regenerate with Claude.")
                    input("\nPress Enter to continue...")
                    continue

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

            append_csv(level, unique_items)
            update_coverage(level, unique_items, state)
            save_state(state)

            write_qs = sum(1 for q in unique_items if q.question_text.startswith("Write: "))
            fill_qs = len(unique_items) - write_qs
            hints_count = sum(1 for q in unique_items if q.hint and not q.question_text.startswith("Write: "))

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

            if done < target:
                seen, topic_counts = load_csv_stats(level)
                next_batch = min(batch_size, target - done)
                next_topics, _ = plan_topics(level, state, next_batch, topic_counts)
                next_prompt = build_claude_prompt(level, next_topics, next_batch, state, topic_counts)
                if copy_to_clipboard(next_prompt):
                    print(f"\n📋 Next prompt already copied to clipboard! (Batch size: {next_batch})")

                input("\nPress Enter to continue to next batch...")
                clear_screen()
                print_separator("=")
                print(f"  ITALIAN MCQ GENERATOR - Level {level}")
                print(f"  Progress: {done}/{target} questions")
                print(f"  Topics: {len(TOPICS_BY_LEVEL[level])}")
                print_separator("=")

        print(f"\n🎉 Level {level} complete! ({done} questions)")
        if levels.index(level) < len(levels) - 1:
            input("\nPress Enter to continue to next level...")


def dry_run(levels: List[str], batch_size: int):
    state = load_state()
    print("\n=== DRY RUN MODE ===")
    print(f"Batch size: {batch_size}")

    for level in levels:
        seen, topic_counts = load_csv_stats(level)
        print(f"\n--- Level {level} ---")
        print(f"Total topics: {len(TOPICS_BY_LEVEL[level])}")
        print(get_coverage_stats(level, topic_counts))

        topics, plan_counts = plan_topics(level, state, batch_size, topic_counts)
        prompt = build_claude_prompt(level, topics, batch_size, state, topic_counts)

        print(f"\nTopics planned: {', '.join(set(topics))}")
        print(f"Question distribution:")
        for topic, count in plan_counts.items():
            marker = " [DIFFICULT]" if topic in DIFFICULT_TOPICS else ""
            print(f"  - {topic}: {count} questions{marker}")

        print(f"\nPrompt length: {len(prompt)} characters")
        prompt_file = save_prompt_to_file(prompt, level)
        print(f"Prompt saved to: {prompt_file}")

        if copy_to_clipboard(prompt):
            print("📋 Prompt copied to clipboard!")

        if input("\nShow full prompt? (y/n): ").lower() == 'y':
            print("\n" + prompt)

    print("\n✅ Dry run complete. Prompts saved to 'prompts' directory.")

# --------------------------------
# CLI
# --------------------------------

def parse_args():
    ap = argparse.ArgumentParser(
        description="Italian MCQ dataset builder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--levels", type=str, default="A1,A2,B1,B2,C1",
                    help="Comma-separated CEFR levels (default: all)")
    ap.add_argument("--target", type=int, default=TARGET_PER_LEVEL,
                    help=f"Target questions per level (default: {TARGET_PER_LEVEL})")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                    help=f"Questions per batch (default: {DEFAULT_BATCH_SIZE})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview prompts and coverage without generating")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()

    levels = [s.strip().upper() for s in args.levels.split(",") if s.strip()]
    for l in levels:
        if l not in LEVELS:
            print(f"❌ Unknown level '{l}'. Valid: {', '.join(LEVELS)}")
            sys.exit(1)

    print("🚀 Italian MCQ Generator")
    print(f"   Levels: {', '.join(levels)}")
    print(f"   Target: {args.target} questions per level")
    print(f"   Batch size: {args.batch_size}")
    print(f"   Min per topic: {MIN_PER_TOPIC}")
    print(f"   Mode: {'DRY RUN' if args.dry_run else 'MANUAL GENERATION'}")

    if args.dry_run:
        dry_run(levels, args.batch_size)
    else:
        input("\nPress Enter to begin...")
        manual_bridge_claude(levels, args.target, args.batch_size)
        print("\n✅ All done!")
