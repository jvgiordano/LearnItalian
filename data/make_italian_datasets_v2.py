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
        "Numeri 0–100 e prezzi",
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
    ],
    "B2": [
        "Congiuntivo presente",
        "Congiuntivo passato",
        "Congiuntivo imperfetto",
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
    "Verbi irregolari comuni (andare, fare, venire, stare, dare, uscire)": """
    Essential high-frequency irregular verbs - must be mastered at A1:
    ANDARE: vado, vai, va, andiamo, andate, vanno
    FARE: faccio, fai, fa, facciamo, fate, fanno
    VENIRE: vengo, vieni, viene, veniamo, venite, vengono
    STARE: sto, stai, sta, stiamo, state, stanno
    DARE: do, dai, dà, diamo, date, danno
    USCIRE: esco, esci, esce, usciamo, uscite, escono
    """,
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

    # 1. Guaranteed slots for topics below MIN_PER_TOPIC
    below_min = sorted(
        [t for t in topics if topic_counts.get(t, 0) < MIN_PER_TOPIC],
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
            shortfall = max(0, MIN_PER_TOPIC - csv_count)
            base_weight = (1.0 + shortfall) if shortfall > 0 else 1.0 / (1.0 + csv_count - MIN_PER_TOPIC + 1)
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
    zero_topics  = [t for t in sorted_topics if topic_counts.get(t, 0) == 0]
    below_min    = [t for t in sorted_topics if 0 < topic_counts.get(t, 0) < MIN_PER_TOPIC]
    at_min       = [t for t in sorted_topics if topic_counts.get(t, 0) >= MIN_PER_TOPIC]

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
        lines.append(f"  ── 🟡 Below {MIN_PER_TOPIC} questions ({len(below_min)} topics) ──")
        for t in below_min[:10]:
            count = topic_counts.get(t, 0)
            shortfall = MIN_PER_TOPIC - count
            marker = " [DIFFICULT]" if t in DIFFICULT_TOPICS else ""
            lines.append(f"    • {t}: {count} (needs {shortfall} more){marker}")
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
        shortfall = max(0, MIN_PER_TOPIC - csv_count)
        if csv_count == 0:
            urgency = f"🔴 MISSING — needs {MIN_PER_TOPIC} questions"
        elif shortfall > 0:
            urgency = f"🟡 LOW — {csv_count} questions (needs {shortfall} more to reach minimum)"
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
