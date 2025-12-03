#!/usr/bin/env python3
"""
Italian MCQ Dataset Generator - Optimized for Claude
Produces high-quality Italian language learning questions with 100% accuracy focus.

Features:
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
STATE_PATH = Path("state_progress.json")
PROMPT_DIR = Path("prompts")
LEVELS = ["A2"]  # Only A2 for now

# Question type ratios
WRITE_RATIO = 0.05  # 5% translation questions
BONUS_RATIO = 0.20  # Up to 20% bonus for difficult topics
BONUS_MAX = 2  # Max 2 bonus items per batch

# --------------------------------
# Topics by Level (Granular approach)
# --------------------------------
TOPICS_BY_LEVEL: Dict[str, List[str]] = {
    "A2": [
        "Imperfetto vs. Passato Prossimo",
        "Preposizioni Semplici",
        "Stare + Gerundio",
        "Verbo Essere e Presente Indicativo",
        "Aggettivi e Pronomi Indefiniti",
        "Condizionale Presente",
        "Pronomi Combinati",
        "Pronomi Relativi",
        "Si Impersonale",
        "Imperativo Formale",
        "Volerci vs. Metterci",
    ],
}

# Topic-specific notes for nuances to cover
TOPIC_NUANCES = {
    "Imperfetto vs. Passato Prossimo": """
    MUST cover these contrasts:
    - Completed Action vs. Ongoing Action
    - Specific Event vs. Habitual Action  
    - Background Description vs. Main Event
    - Modal Verbs (usage with imperfetto vs. passato prossimo)
    Examples: "Mentre mangiavo, è arrivato" (background vs. event)
             "Da bambino andavo sempre al mare" (habitual) vs. "Ieri sono andato al mare" (specific)
    """,
    
    "Pronomi Combinati": """
    Focus on combined pronouns like:
    - me lo, te la, glielo, ce li, ve le, etc.
    - Word order and attachment to infinitives
    - Changes with imperatives
    """,
    
    "Pronomi Relativi": """
    Cover relative pronouns:
    - che (that/which/who)
    - cui (which/whom - used after prepositions)
    - il quale/la quale/i quali/le quali
    - chi (those who/whoever)
    """,
    
    "Si Impersonale": """
    Cover impersonal constructions:
    - Si mangia bene (One eats well)
    - Si dice che... (It is said that...)
    - Agreement with plural nouns (Si vendono case)
    """,
    
    "Imperativo Formale": """
    Focus on formal imperative (Lei form):
    - Venga qui! (Come here!)
    - Non parli! (Don't speak!)
    - With pronouns: Me lo dia! (Give it to me!)
    """,
    
    "Volerci vs. Metterci": """
    Distinguish between:
    - Volerci (impersonal - time/things needed): Ci vogliono due ore
    - Metterci (personal - time someone takes): Ci metto due ore
    """,
    
    "Stare + Gerundio": """
    Present progressive construction:
    - Sto mangiando (I am eating)
    - Formation of gerunds (-ando, -endo)
    - Irregular gerunds (fare → facendo, dire → dicendo)
    """,
    
    "Preposizioni Semplici": """
    Focus on:
    - di, a, da, in, con, su, per, tra/fra
    - Common usage patterns
    - Verbs that require specific prepositions
    """,
    
    "Condizionale Presente": """
    Cover:
    - Formation (regular and irregular)
    - Polite requests: Vorrei, potrebbe
    - Hypothetical situations
    - Advice and suggestions
    """,
    
    "Aggettivi e Pronomi Indefiniti": """
    Include:
    - qualche, alcuni/e, ogni, tutto, nessuno
    - qualcuno, qualcosa, niente, nulla
    - Agreement patterns
    """,
    
    "Verbo Essere e Presente Indicativo": """
    Review fundamentals:
    - All forms of essere
    - Present tense regular verbs (-are, -ere, -ire)
    - Common irregular verbs (avere, fare, andare, etc.)
    """
}

# Topic-specific resource URLs (expand as needed)
TOPIC_RESOURCES = {
    "Passato prossimo": "https://www.lawlessitalian.com/grammar/passato-prossimo/",
    "Imperfetto": "https://www.lawlessitalian.com/grammar/imperfetto/",
    "Congiuntivo presente": "https://www.lawlessitalian.com/grammar/subjunctive-present/",
    "Articoli": "https://www.lawlessitalian.com/grammar/articles/",
    "Verbo essere": "https://www.lawlessitalian.com/grammar/essere-to-be/",
    "Verbo avere": "https://www.lawlessitalian.com/grammar/avere-to-have/",
    # Add more specific mappings as discovered
}

# Difficult topics that often need more practice
DIFFICULT_TOPICS = {
    "Imperfetto vs. Passato Prossimo",
    "Pronomi Combinati",
    "Si Impersonale",
    "Volerci vs. Metterci",
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
    stats.append(f"Coverage for {level}:")
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

# --------------------------------
# Validation
# --------------------------------

def validate_question(item: dict, level: str, index: int) -> List[str]:
    """Comprehensive validation of a single question."""
    errors = []
    
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in item or item[field] is None:
            errors.append(f"Missing field: {field}")
            return errors  # Can't continue without all fields
        if isinstance(item[field], str):
            item[field] = item[field].strip()
    
    qt = item["question_text"]
    cs = item["complete_sentence"]
    
    # Determine question type
    is_write = qt.startswith("Write: ")
    
    if is_write:
        # Write question validation
        if "___" in qt:
            errors.append("Write: questions must not contain ___")
        
        english_part = qt[7:].strip()
        if not english_part:
            errors.append("Write: question missing English text")
        
        # For Write questions, complete_sentence should be the Italian translation
        if not cs:
            errors.append("Write: question missing Italian translation in complete_sentence")
            
    else:
        # Fill-in-the-blank validation
        blank_count = qt.count("___")
        if blank_count != 1:
            errors.append(f"Must have exactly one ___ blank (found {blank_count})")
        
        # Validate reconstruction
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
    
    # Check for empty options
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
    
    # Hint validation (for fill-in questions)
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

# --------------------------------
# CSV Operations
# --------------------------------

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

# --------------------------------
# UI Functions
# --------------------------------

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def copy_to_clipboard(text: str) -> bool:
    """Try to copy text to clipboard."""
    try:
        pyperclip.copy(text)
        return True
    except:
        return False

def print_separator(char="=", width=80):
    """Print a visual separator."""
    print(char * width)

def save_prompt_to_file(prompt: str, level: str) -> Path:
    """Save prompt to a file for easy access."""
    PROMPT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROMPT_DIR / f"prompt_{level}_{timestamp}.txt"
    filepath.write_text(prompt, encoding="utf-8")
    return filepath

# --------------------------------
# Main Workflow
# --------------------------------

def manual_bridge_claude(levels: List[str], target: int, batch_size: int):
    """Manual workflow optimized for Claude."""
    state = load_state()
    
    for level in levels:
        seen = load_existing_texts(level)
        done = len(seen)
        
        clear_screen()
        print_separator("=")
        print(f"  ITALIAN MCQ GENERATOR - Level {level}")
        print(f"  Progress: {done}/{target} questions")
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
                print(f"  ITALIAN MCQ GENERATOR - Level {level}")
                print(f"  Progress: {done}/{target} questions")
                print_separator("=")
        
        print(f"\n🎉 Level {level} complete! ({done} questions)")
        if levels.index(level) < len(levels) - 1:
            input("\nPress Enter to continue to next level...")

# --------------------------------
# Dry Run Mode
# --------------------------------

def dry_run(levels: List[str], batch_size: int):
    """Preview prompts and coverage without generating."""
    state = load_state()
    
    print("\n=== DRY RUN MODE ===")
    print(f"Batch size: {batch_size}")
    
    for level in levels:
        print(f"\n--- Level {level} ---")
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

# --------------------------------
# CLI
# --------------------------------

def parse_args():
    ap = argparse.ArgumentParser(
        description="Italian MCQ dataset builder optimized for Claude",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --dry-run --levels A2 --batch-size 10
  %(prog)s --levels A2 --target 100 --batch-size 8
  %(prog)s --levels A2 --batch-size 5  # For maximum quality
        """
    )
    ap.add_argument("--levels", type=str, default="A2",
                    help="Comma-separated CEFR levels (default: A2)")
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
    
    print("🚀 Italian MCQ Generator for Claude")
    print(f"    Levels: {', '.join(levels)}")
    print(f"    Target: {args.target} questions per level")
    print(f"    Batch size: {args.batch_size} (reduce for higher quality)")
    print(f"    Mode: {'DRY RUN' if args.dry_run else 'MANUAL GENERATION'}")
    
    if args.batch_size > 10:
        print("\n⚠️  Warning: Batch sizes > 10 may reduce quality. Consider using --batch-size 5-8")
    
    if args.dry_run:
        dry_run(levels, args.batch_size)
    else:
        print("\n📌 Note: This script prioritizes 100% accuracy over speed.")
        print("    Invalid questions will be completely rejected.")
        input("\nPress Enter to begin...")
        manual_bridge_claude(levels, args.target, args.batch_size)
        print("\n✅ All done! Check the 'data' directory for your CSV files.")