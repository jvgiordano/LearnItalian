import customtkinter as ctk
import sqlite3
import json
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates
import numpy as np
import uuid
from collections import defaultdict
import os
import re
import unicodedata
from difflib import SequenceMatcher
import webbrowser

# --- App Settings ---
APP_NAME = "Progress with Italian"
WIDTH = 1400
HEIGHT = 900

MASTERY_HISTORY_LENGTH = 20
MASTERY_THRESHOLD = 6  # Questions become rare after 6 correct answers
MIN_TOPIC_QUESTIONS = 3  # Minimum questions per topic in adaptive quiz

class ImprovedAdaptiveLearningEngine:
    def __init__(self):
        self.levels = ['A0', 'A1', 'A2', 'B1', 'B2', 'C1']
        # Coverage requirement for level progression - UPDATED to 85%
        self.MIN_COVERAGE_FOR_PROGRESSION = 0.30 # This is now legacy, but kept for other calculations
        # Ensure database has required columns
        self.ensure_database_schema()

    def get_recent_quiz_performance(self, level, last_n_quizzes=5):
        """
        Calculate average performance from the last N quizzes for a specific level.
        Returns a score between 0 and 1 (percentage).
        """
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        
        # Get questions from the specified level
        cursor.execute("SELECT id FROM questions WHERE cefr_level = ?", (level,))
        level_question_ids = [row[0] for row in cursor.fetchall()]
        
        if not level_question_ids:
            conn.close()
            return 0.0
        
        # Get recent answer history for these questions
        placeholders = ','.join('?' for _ in level_question_ids)
        cursor.execute(f"""
            SELECT ah.is_correct, ah.timestamp
            FROM answer_history ah
            WHERE ah.question_id IN ({placeholders})
            ORDER BY ah.timestamp DESC
            LIMIT ?
        """, level_question_ids + [last_n_quizzes * 10])  # Assuming ~10 questions per quiz
        
        recent_answers = cursor.fetchall()
        conn.close()
        
        if not recent_answers:
            return 0.0  # No history, treat as low performance
        
        # Calculate success rate
        correct_count = sum(1 for answer in recent_answers if answer[0] == 1)
        total_count = len(recent_answers)
        
        return correct_count / total_count if total_count > 0 else 0.0


    def ensure_database_schema(self):
        """Ensure the database has all required columns and tables."""
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        
        try:
            # 1. Ensure 'enhanced_performance' table has freeform columns and unanswered column
            cursor.execute('PRAGMA table_info(enhanced_performance)')
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'freeform_correct_count' not in columns:
                cursor.execute('ALTER TABLE enhanced_performance ADD COLUMN freeform_correct_count INTEGER DEFAULT 0')
            if 'freeform_incorrect_count' not in columns:
                cursor.execute('ALTER TABLE enhanced_performance ADD COLUMN freeform_incorrect_count INTEGER DEFAULT 0')
            if 'partial_correct_count' not in columns:
                cursor.execute('ALTER TABLE enhanced_performance ADD COLUMN partial_correct_count INTEGER DEFAULT 0')
            # NEW: Add unanswered_count column
            if 'unanswered_count' not in columns:
                cursor.execute('ALTER TABLE enhanced_performance ADD COLUMN unanswered_count INTEGER DEFAULT 0')

            # 2. Ensure 'quiz_history' table exists and has the 'session_id' column
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS quiz_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    score INTEGER NOT NULL,
                    total_questions INTEGER NOT NULL,
                    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('PRAGMA table_info(quiz_history)')
            quiz_history_columns = [col[1] for col in cursor.fetchall()]

            if 'session_id' not in quiz_history_columns:
                cursor.execute('ALTER TABLE quiz_history ADD COLUMN session_id TEXT')

            # 3. Create daily_stats table for Progress Timeline
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date DATE PRIMARY KEY,
                    total_coverage REAL DEFAULT 0,
                    total_mastery REAL DEFAULT 0,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 4. Create answer_history table for recency calculations
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS answer_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question_id INTEGER NOT NULL,
                    cefr_level TEXT NOT NULL,
                    is_correct INTEGER NOT NULL, -- 1 for true, 0 for false
                    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (question_id) REFERENCES questions(id)
                )
            ''')

            conn.commit()
        except Exception as e:
            print(f"Database schema update/check error: {e}")
        finally:
            conn.close()

    def get_next_level(self, current_level):
        """Gets the next CEFR level, handles the max level case."""
        try:
            current_index = self.levels.index(current_level)
            if current_index + 1 < len(self.levels):
                return self.levels[current_index + 1]
            else:
                return current_level  # Already at max level
        except ValueError:
            return self.levels[1] # Default to A1 if something goes wrong

    def assess_user_level_and_topics(self):
        """
        Comprehensive assessment of user's current level and topic strengths/weaknesses.
        This method is now primarily for displaying stats and weaknesses, while the actual
        level calculation is handled by the more complex calculate_estimated_level.
        """
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT q.cefr_level, q.topic,
                       SUM(IFNULL(ep.correct_count, 0)) as correct,
                       SUM(IFNULL(ep.incorrect_count, 0)) as incorrect,
                       SUM(IFNULL(ep.freeform_correct_count, 0)) as freeform_correct,
                       SUM(IFNULL(ep.freeform_incorrect_count, 0)) as freeform_incorrect,
                       SUM(IFNULL(ep.unanswered_count, 0)) as unanswered
                FROM questions q
                LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                GROUP BY q.cefr_level, q.topic
                HAVING (correct + incorrect + freeform_correct + freeform_incorrect + unanswered) > 0
            ''')
            performance_data = cursor.fetchall()
        except Exception:
            # Fallback if unanswered_count doesn't exist yet
            try:
                cursor.execute('''
                    SELECT q.cefr_level, q.topic,
                           SUM(IFNULL(ep.correct_count, 0)) as correct,
                           SUM(IFNULL(ep.incorrect_count, 0)) as incorrect,
                           SUM(IFNULL(ep.freeform_correct_count, 0)) as freeform_correct,
                           SUM(IFNULL(ep.freeform_incorrect_count, 0)) as freeform_incorrect,
                           0 as unanswered
                    FROM questions q
                    LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                    GROUP BY q.cefr_level, q.topic
                    HAVING (correct + incorrect + freeform_correct + freeform_incorrect) > 0
                ''')
                performance_data = cursor.fetchall()
            except Exception:
                conn.close()
                return {'estimated_level': 'A0', 'topic_weaknesses': [], 'coverage_percentages': {}}

        topic_weaknesses = []
        coverage_percentages = {}
        
        for row in performance_data:
            level = row['cefr_level']
            topic = row['topic']
            weighted_correct = row['correct'] + (row['freeform_correct'] * 1.5)
            weighted_incorrect = row['incorrect'] + (row['freeform_incorrect'] * 1.5)
            weighted_total = weighted_correct + weighted_incorrect
            
            if weighted_total >= 1:
                success_rate = weighted_correct / weighted_total if weighted_total > 0 else 0
                if success_rate < 0.7:
                    topic_weaknesses.append({
                        'topic': topic, 'level': level, 'success_rate': success_rate
                    })
        
        for level in self.levels:
            if level == 'A0': continue
            coverage_percentages[level] = self.get_coverage_percentage(level)
        
        # Use the new, more robust calculation method for the final estimation
        estimated_level = self.calculate_estimated_level()
        
        conn.close()
        
        return {
            'estimated_level': estimated_level,
            'topic_weaknesses': topic_weaknesses,
            'coverage_percentages': coverage_percentages
        }

    # FIXED: Helper method for sustained success check
    def _check_sustained_success(self, cursor, level):
        """Check for 85% success over last 50 Qs, sustained for 25 consecutive checks."""
        cursor.execute(
            "SELECT is_correct FROM answer_history WHERE cefr_level = ? ORDER BY timestamp DESC LIMIT 100",
            (level,)
        )
        # Fetch results and reverse to get chronological order
        results = [row[0] for row in cursor.fetchall()]
        results.reverse()

        if len(results) < 50:
            return False

        sustain_counter = 0
        # Loop through the results to check the moving window
        # We start checking from the 50th answer
        for i in range(49, len(results)):
            window = results[i-49 : i+1]
            success_rate = sum(window) / 50.0
            
            if success_rate >= 0.85:
                sustain_counter += 1
                if sustain_counter >= 25:
                    return True # Achieved 25 consecutive successful checks
            else:
                sustain_counter = 0 # Reset if the condition fails
        
        return False

    def get_sustained_success_streak(self, level):
        """FIXED: Returns the current sustained success streak for a given level."""
        if level == 'A0': return 0
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT is_correct FROM answer_history WHERE cefr_level = ? ORDER BY timestamp DESC LIMIT 100",
            (level,)
        )
        results = [row[0] for row in cursor.fetchall()]
        results.reverse()
        conn.close()

        if len(results) < 50:
            return 0

        sustain_counter = 0
        max_sustain_counter = 0  # Track the highest streak achieved
        
        for i in range(49, len(results)):
            window = results[i-49 : i+1]
            success_rate = sum(window) / 50.0
            
            if success_rate >= 0.85:
                sustain_counter += 1
                max_sustain_counter = max(max_sustain_counter, sustain_counter)
            else:
                sustain_counter = 0
        
        # Return the current sustained counter (not max) for accurate progress tracking
        return sustain_counter

    # UPDATED: Helper method for topic coverage check - now 85% instead of 90%
    def _check_topic_coverage(self, cursor, level):
        """Check if at least 85% of topics in a level have been attempted."""
        coverage_percentage = self.get_level_topic_coverage(level, cursor)
        return coverage_percentage >= 0.85  # CHANGED from 0.90 to 0.85

    def get_level_topic_coverage(self, level, cursor_obj=None):
        """Returns the topic coverage percentage for a given level."""
        if level == 'A0': return 0.0
        conn = None
        if cursor_obj is None:
            conn = sqlite3.connect('italian_quiz.db')
            cursor = conn.cursor()
        else:
            cursor = cursor_obj

        cursor.execute("SELECT COUNT(DISTINCT topic) FROM questions WHERE cefr_level = ?", (level,))
        total_topics = cursor.fetchone()[0]
        
        if total_topics == 0:
            if conn: conn.close()
            return 1.0

        cursor.execute("""
            SELECT COUNT(DISTINCT q.topic) 
            FROM questions q 
            JOIN answer_history ah ON q.id = ah.question_id 
            WHERE q.cefr_level = ?
        """, (level,))
        answered_topics = cursor.fetchone()[0]

        if conn: conn.close()
        
        return answered_topics / total_topics if total_topics > 0 else 0

    # Helper method for mastery score check
    def _check_mastery_score(self, cursor, level):
        """Check if the overall Mastery Score for a level is at least 50%."""
        stats = self._calculate_mastery_for_level(level, cursor)
        return stats['mastery_value'] >= 0.50
    
    def get_level_mastery_score(self, level):
        """Returns the mastery score for a given level."""
        if level == 'A0': return 0.0
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        stats = self._calculate_mastery_for_level(level, cursor)
        conn.close()
        return stats['mastery_value']

    # REWRITTEN: The core logic for CEFR level advancement
    def calculate_estimated_level(self):
        """
        Calculates user's CEFR level based on the new 3-criteria model:
        1. Sustained recent success (85% over 50 Qs, for 25 checks)
        2. Broad topic coverage (85%) - UPDATED from 90%
        3. Minimum mastery score (50%)
        """
        highest_mastered = 'A0' # Default level
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check each level independently to allow for non-sequential progression
        for level in self.levels:
            if level == 'A0': continue # Can't master A0
            
            # Check all three conditions for the current level
            cond1_sustained_success = self._check_sustained_success(cursor, level)
            cond2_topic_coverage = self._check_topic_coverage(cursor, level)
            cond3_mastery_score = self._check_mastery_score(cursor, level)

            if cond1_sustained_success and cond2_topic_coverage and cond3_mastery_score:
                # If all conditions are met, this level is considered mastered
                if self.levels.index(level) >= self.levels.index(highest_mastered):
                    highest_mastered = level
        
        conn.close()
        return highest_mastered

    def get_level_distribution(self, user_level, total_questions=10, user_assessment=None):
        """
        Determine how many questions should come from each level.
        - 5-9 questions from the user's level, adjusted by mastery.
        - Special case for C1 users to ensure broader review.
        """
        user_level_index = self.levels.index(user_level) if user_level in self.levels else 0
        distribution = {level: 0 for level in self.levels if level != 'A0'}

        # Special case for C1 users
        if user_level == 'C1':
            user_level_count = random.randint(3, 6)
        else:
            # Adjust question count based on coverage/mastery
            coverage = user_assessment['coverage_percentages'].get(user_level, 0)
            if coverage < 0.3:  # Low mastery, focus on current level
                user_level_count = random.randint(8, 9)
            elif coverage < 0.7: # Medium mastery, start introducing more variety
                user_level_count = random.randint(6, 7)
            else: # High mastery, reduce current level questions
                user_level_count = random.randint(5, 6)

        distribution[user_level] = min(total_questions, user_level_count)
        remaining = total_questions - distribution[user_level]

        # Distribute remaining questions
        if remaining > 0:
            if user_level == 'A1':  # Working towards A1 (from A0)
                # For beginners, only introduce a small amount of next level
                distribution['A2'] = min(remaining, random.randint(1, 2))
                # Fill any remaining slots with more A1 questions
                if remaining > distribution['A2']:
                    distribution['A1'] += remaining - distribution['A2']
            elif user_level == 'A2': # Working towards A2 (from A1)
                distribution['A1'] += max(1, int(remaining * 0.3))
                distribution['B1'] = remaining - distribution['A1']
            elif user_level == 'B1':
                distribution['A2'] += max(1, int(remaining * 0.3))
                distribution['B2'] = remaining - distribution['A2']
            elif user_level == 'B2':
                distribution['B1'] += max(1, int(remaining * 0.4))
                distribution['C1'] = remaining - distribution['B1']
            elif user_level == 'C1':
                distribution['B2'] += max(1, int(remaining * 0.5))
                distribution['B1'] += max(1, int(remaining * 0.3))
                distribution['A2'] += remaining - (distribution['B2'] + distribution['B1'])
        
        # Ensure total is correct
        current_total = sum(distribution.values())
        if current_total != total_questions:
            diff = total_questions - current_total
            distribution[user_level] += diff

        return distribution
    
    
    def get_questions_for_level(self, level, count, user_assessment, topics=None, exclude_recent_hours=24, topic_counts=None, max_per_topic=3):
        """
        Get questions for a specific level, prioritizing topic weaknesses and avoiding recent questions
        NOW WITH: Respects maximum questions per topic limit
        FIXED: Includes hint and alternate_correct_responses columns
        """
        if count <= 0:
            return []
        
        if topic_counts is None:
            topic_counts = {}
            
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Build query with filters - FIXED: Added hint and alternate_correct_responses
        base_query = '''
            SELECT q.id, q.question_text, q.english_translation, q.option_a, q.option_b, 
                   q.option_c, q.option_d, q.correct_option, q.cefr_level, q.topic, q.explanation, q.resource,
                   q.hint, q.alternate_correct_responses,
                   IFNULL(ep.correct_count, 0) as correct_count,
                   IFNULL(ep.incorrect_count, 0) as incorrect_count,
                   ep.last_seen
            FROM questions q
            LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
            WHERE q.cefr_level = ?
        '''
        
        params = [level]
        
        # Filter by topics if specified
        if topics:
            placeholders = ','.join('?' for _ in topics)
            base_query += f" AND q.topic IN ({placeholders})"
            params.extend(topics)
        
        # Exclude topics that already have max_per_topic questions
        maxed_topics = [topic for topic, cnt in topic_counts.items() if cnt >= max_per_topic]
        if maxed_topics:
            placeholders = ','.join('?' for _ in maxed_topics)
            base_query += f" AND q.topic NOT IN ({placeholders})"
            params.extend(maxed_topics)
        
        # Exclude questions seen very recently (especially wrong ones)
        cutoff_time = (datetime.now() - timedelta(hours=exclude_recent_hours)).isoformat()
        base_query += """ AND (
            ep.last_seen IS NULL 
            OR ep.last_seen < ?
            OR (ep.last_seen < ? AND ep.incorrect_count = 0)
        )"""
        params.extend([cutoff_time, cutoff_time])
        
        cursor.execute(base_query, params)
        available_questions = cursor.fetchall()
        conn.close()
        
        if not available_questions:
            return []
        
        # Convert to list for easier manipulation
        questions = [dict(q) for q in available_questions]
        
        # Group questions by topic
        questions_by_topic = {}
        for q in questions:
            topic = q['topic']
            if topic not in questions_by_topic:
                questions_by_topic[topic] = []
            questions_by_topic[topic].append(q)
        
        # Prioritize questions based on multiple factors
        for topic_questions in questions_by_topic.values():
            for q in topic_questions:
                priority_score = self.calculate_question_priority_v2(q, user_assessment, level)
                q['priority_score'] = priority_score
            
            # Group questions by priority score (to handle ties)
            priority_groups = {}
            for q in topic_questions:
                # Round to 1 decimal to group very similar scores
                score_key = round(q['priority_score'], 1)
                if score_key not in priority_groups:
                    priority_groups[score_key] = []
                priority_groups[score_key].append(q)
            
            # Rebuild topic_questions with randomization within priority groups
            topic_questions.clear()
            for score_key in sorted(priority_groups.keys(), reverse=True):
                group = priority_groups[score_key]
                random.shuffle(group)  # Randomize questions with same/similar priority
                topic_questions.extend(group)
        
        # Select questions while respecting topic limits
        selected = []
        topic_selection_counts = dict(topic_counts)  # Work with a copy
        
        # Create a single list of all questions sorted by priority
        all_questions_sorted = []
        for topic_questions in questions_by_topic.values():
            all_questions_sorted.extend(topic_questions)
        
        # Sort ALL questions by priority score (highest first)
        all_questions_sorted.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Select highest priority questions while respecting topic limit
        for q in all_questions_sorted:
            if len(selected) >= count:
                break
            
            topic = q['topic']
            current_count = topic_selection_counts.get(topic, 0)
            
            # Only add if we haven't hit the limit for this topic
            if current_count < max_per_topic:
                selected.append(q)
                topic_selection_counts[topic] = current_count + 1
        
        # Add some randomization to avoid always getting the same "highest priority" questions
        # But maintain some priority ordering
        if len(selected) > 3:
            # Shuffle within priority groups
            high_priority = selected[:len(selected)//3]
            mid_priority = selected[len(selected)//3:2*len(selected)//3]
            low_priority = selected[2*len(selected)//3:]
            
            random.shuffle(high_priority)
            random.shuffle(mid_priority)
            random.shuffle(low_priority)
            
            selected = high_priority + mid_priority + low_priority
        
        return selected[:count]
           
    def calculate_question_priority_v2(self, question_data, user_assessment, target_level):
        """
        Calculate priority score focusing on learning objectives rather than repetition
        """
        topic = question_data['topic']
        correct = question_data.get('correct_count', 0)
        incorrect = question_data.get('incorrect_count', 0)
        last_seen = question_data.get('last_seen')
        
        # Base score starts neutral
        base_score = 1.0
        
        # Topic weakness factor (highest priority)
        topic_factor = 1.0
        for weakness in user_assessment['topic_weaknesses']:
            if weakness['topic'] == topic and weakness['level'] == target_level:
                topic_factor = 3.0  # High priority for weak topics
                break
        
        # Freshness factor - prefer questions not seen recently or never seen
        freshness_factor = 1.0
        if not last_seen:
            freshness_factor = 2.0  # Never seen before
        else:
            last_seen_dt = datetime.fromisoformat(last_seen)
            hours_since = (datetime.now() - last_seen_dt).total_seconds() / 3600
            
            if hours_since > 168:  # More than a week
                freshness_factor = 1.8
            elif hours_since > 72:  # More than 3 days
                freshness_factor = 1.5
            elif hours_since > 24:  # More than a day
                freshness_factor = 1.2
            else:  # Very recent
                freshness_factor = 0.3
        
        # Performance factor - slightly favor questions with some history but not mastered
        performance_factor = 1.0
        total_attempts = correct + incorrect
        
        if total_attempts == 0:
            performance_factor = 1.5  # New questions are good
        elif total_attempts >= 3 and correct >= 2 and (correct / total_attempts) >= 0.8:
            performance_factor = 0.4  # Mastered questions get low priority
        elif incorrect > correct:
            performance_factor = 0.6  # Questions they struggle with get lower priority to avoid frustration
        else:
            performance_factor = 1.0  # Balanced performance
        
        # Combine factors
        final_score = base_score * topic_factor * freshness_factor * performance_factor
        
        return final_score
    
    def check_question_similarity(self, q1_text, q2_text, threshold=0.5):
        """
        Check if two questions are too similar to appear in the same quiz.
        Returns True if questions are too similar.
        """
        # Normalize both questions for comparison
        q1_normalized = self.normalize_text_for_comparison(q1_text)
        q2_normalized = self.normalize_text_for_comparison(q2_text)
        
        # Quick check: if questions share many of the same words
        q1_words = set(q1_normalized.split())
        q2_words = set(q2_normalized.split())
        
        # Filter out very common words (Italian stop words)
        stop_words = {'il', 'la', 'i', 'le', 'un', 'una', 'di', 'da', 'in', 'su', 
                      'per', 'con', 'a', 'e', 'o', 'ma', 'che', 'non', 'si', 'mi', 
                      'ti', 'ci', 'vi', 'lo', 'gli', 'ne', 'dopo', 'ogni', 'tutto'}
        
        q1_meaningful = q1_words - stop_words
        q2_meaningful = q2_words - stop_words
        
        # If questions share too many meaningful words, they're similar
        if len(q1_meaningful) > 2 and len(q2_meaningful) > 2:
            shared_words = q1_meaningful.intersection(q2_meaningful)
            smaller_set = min(len(q1_meaningful), len(q2_meaningful))
            if len(shared_words) / smaller_set > 0.65:
                return True
        
        # Also check overall string similarity
        similarity = SequenceMatcher(None, q1_normalized, q2_normalized).ratio()
        return similarity > threshold



    def fetch_adaptive_questions(self, number=10, user_assessment=None, level=None, topics=None):
        """
        MODIFIED: Intelligently reserves 0-3 slots for uncovered topics based on recent performance
        AND restricts fallback questions to appropriate levels only
        """
        if user_assessment is None:
            user_assessment = self.assess_user_level_and_topics()
        
        # The user's mastered level
        user_level = user_assessment['estimated_level']
        
        # The level the user is WORKING ON is the next one up
        target_level = self.get_next_level(user_level)
        
        # Override target level if a specific level is requested for a non-adaptive quiz
        if level:
            target_level = level
        
        # Get distribution of questions across levels, centered on the target level
        distribution = self.get_level_distribution(target_level, number, user_assessment)
        
        # First, identify uncovered topics for the target level
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Find topics in target level that have never been answered
        cursor.execute("""
            SELECT DISTINCT q.topic 
            FROM questions q
            WHERE q.cefr_level = ?
            AND q.topic NOT IN (
                SELECT DISTINCT q2.topic 
                FROM questions q2
                JOIN enhanced_performance ep ON q2.id = ep.question_id
                WHERE q2.cefr_level = ?
                AND (ep.correct_count > 0 OR ep.incorrect_count > 0 OR 
                     ep.freeform_correct_count > 0 OR ep.freeform_incorrect_count > 0)
            )
        """, (target_level, target_level))
        
        uncovered_topics = [row['topic'] for row in cursor.fetchall()]
        conn.close()
        
        all_selected_questions = []
        topic_counts = {}  # Track how many questions we have from each topic
        selected_question_texts = []  # Track question texts for similarity checking
        
        # Determine how many new topic questions to include based on recent performance
        new_topic_count = 0
        if uncovered_topics and distribution.get(target_level, 0) > 0:
            recent_performance = self.get_recent_quiz_performance(target_level, last_n_quizzes=5)
            
            if recent_performance >= 0.85:
                # Excellent performance - be more adventurous
                new_topic_count = min(3, len(uncovered_topics), distribution.get(target_level, 0))
            elif recent_performance >= 0.70:
                # Good performance - moderate exploration
                new_topic_count = min(2, len(uncovered_topics), distribution.get(target_level, 0))
            else:
                # Below 70% - focus on consolidation, no forced new topics
                new_topic_count = 0
        
        # Reserve slots for new topic questions if performance warrants it
        if new_topic_count > 0:
            # Randomly select which uncovered topics to introduce
            topics_to_introduce = random.sample(uncovered_topics, 
                                               min(new_topic_count, len(uncovered_topics)))
            
            for new_topic in topics_to_introduce:
                topic_questions = self.get_questions_for_level(
                    target_level, 1, user_assessment, topics=[new_topic],
                    topic_counts=topic_counts, max_per_topic=1
                )
                
                if topic_questions:
                    all_selected_questions.extend(topic_questions)
                    selected_question_texts.extend([q['question_text'] for q in topic_questions])
                    topic_counts[new_topic] = 1
                    # Reduce the count for target level since we've added questions
                    distribution[target_level] -= 1
                    
                    # Stop if we've filled our new topic quota
                    if len(all_selected_questions) >= new_topic_count:
                        break
        
        # Get questions for each level according to remaining distribution
        for dist_level, count in distribution.items():
            if count > 0:
                # Get more candidates than needed to account for similarity filtering
                candidates_needed = count * 3  # Get 3x candidates
                level_questions = self.get_questions_for_level(
                    dist_level, candidates_needed, user_assessment, topics, 
                    topic_counts=topic_counts,
                    max_per_topic=2
                )
                
                # Filter out questions that are too similar to already selected ones
                for q in level_questions:
                    # Check similarity with all already selected questions
                    is_similar = False
                    for selected_text in selected_question_texts:
                        if self.check_question_similarity(q['question_text'], selected_text):
                            is_similar = True
                            break
                    
                    if not is_similar and len(all_selected_questions) < number:
                        all_selected_questions.append(q)
                        selected_question_texts.append(q['question_text'])
                        # Update topic counts
                        topic = q['topic']
                        topic_counts[topic] = topic_counts.get(topic, 0) + 1
                        
                        # Stop if we have enough questions for this level
                        level_count = len([q for q in all_selected_questions if q['cefr_level'] == dist_level])
                        if level_count >= count:
                            break
        
        # If we don't have enough questions due to similarity filtering, get more
        if len(all_selected_questions) < number:
            remaining_needed = number - len(all_selected_questions)
            existing_ids = {q['id'] for q in all_selected_questions}
            
            conn = sqlite3.connect('italian_quiz.db')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get additional candidates - NOW WITH LEVEL RESTRICTIONS
            maxed_topics = [topic for topic, count in topic_counts.items() if count >= 3]
            
            # Determine appropriate levels for fallback based on target level
            if target_level == 'A1':
                allowed_levels = ['A1', 'A2']
            elif target_level == 'A2':
                allowed_levels = ['A1', 'A2', 'B1']
            elif target_level == 'B1':
                allowed_levels = ['A2', 'B1', 'B2']
            elif target_level == 'B2':
                allowed_levels = ['B1', 'B2', 'C1']
            elif target_level == 'C1':
                allowed_levels = ['B1', 'B2', 'C1']
            else:
                allowed_levels = ['A1', 'A2']  # Default safe fallback
            
            level_placeholders = ','.join('?' for _ in allowed_levels)
            
            if existing_ids:
                if maxed_topics:
                    query = """SELECT *, hint, alternate_correct_responses FROM questions 
                              WHERE id NOT IN ({}) AND topic NOT IN ({}) AND cefr_level IN ({})
                              ORDER BY RANDOM() LIMIT ?""".format(
                        ','.join('?' for _ in existing_ids),
                        ','.join('?' for _ in maxed_topics),
                        level_placeholders
                    )
                    params = list(existing_ids) + maxed_topics + allowed_levels + [remaining_needed * 5]
                else:
                    query = """SELECT *, hint, alternate_correct_responses FROM questions 
                              WHERE id NOT IN ({}) AND cefr_level IN ({})
                              ORDER BY RANDOM() LIMIT ?""".format(
                        ','.join('?' for _ in existing_ids),
                        level_placeholders
                    )
                    params = list(existing_ids) + allowed_levels + [remaining_needed * 5]
            else:
                if maxed_topics:
                    query = """SELECT *, hint, alternate_correct_responses FROM questions 
                              WHERE topic NOT IN ({}) AND cefr_level IN ({})
                              ORDER BY RANDOM() LIMIT ?""".format(
                        ','.join('?' for _ in maxed_topics),
                        level_placeholders
                    )
                    params = maxed_topics + allowed_levels + [remaining_needed * 5]
                else:
                    query = """SELECT *, hint, alternate_correct_responses FROM questions 
                              WHERE cefr_level IN ({})
                              ORDER BY RANDOM() LIMIT ?""".format(level_placeholders)
                    params = allowed_levels + [remaining_needed * 5]
            
            cursor.execute(query, params)
            additional_candidates = [dict(q) for q in cursor.fetchall()]
            
            # Filter additional candidates for similarity and topic limits
            for q in additional_candidates:
                # Check similarity
                is_similar = False
                for selected_text in selected_question_texts:
                    if self.check_question_similarity(q['question_text'], selected_text):
                        is_similar = True
                        break
                
                # Check topic limit
                if not is_similar and topic_counts.get(q['topic'], 0) < 3:
                    all_selected_questions.append(q)
                    selected_question_texts.append(q['question_text'])
                    topic_counts[q['topic']] = topic_counts.get(q['topic'], 0) + 1
                    if len(all_selected_questions) >= number:
                        break
            
            conn.close()
        
        # Shuffle the final selection to avoid predictable ordering
        random.shuffle(all_selected_questions)
        
        return all_selected_questions[:number]


    def get_coverage_percentage(self, level):
        """Get coverage percentage for a specific CEFR level"""
        if level == 'A0': return 0.0
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Total questions for this level
        cursor.execute("SELECT COUNT(*) AS total FROM questions WHERE cefr_level = ?", (level,))
        total_q = cursor.fetchone()['total']
        
        # Questions answered correctly at least once (including freeform)
        try:
            cursor.execute('''
                SELECT COUNT(DISTINCT q.id) AS answered 
                FROM questions q 
                JOIN enhanced_performance ep ON q.id = ep.question_id 
                WHERE q.cefr_level = ? AND (ep.correct_count > 0 OR ep.freeform_correct_count > 0)
            ''', (level,))
        except:
            # Fallback if freeform columns don't exist
            cursor.execute('''
                SELECT COUNT(DISTINCT q.id) AS answered 
                FROM questions q 
                JOIN enhanced_performance ep ON q.id = ep.question_id 
                WHERE q.cefr_level = ? AND ep.correct_count > 0
            ''', (level,))
        
        answered_q = cursor.fetchone()['answered'] or 0
        
        conn.close()
        
        coverage = (answered_q / total_q) if total_q > 0 else 0
        return coverage
    
    def get_total_coverage_and_mastery(self, cursor):
        """Calculate total coverage and mastery across all levels"""
        cursor.execute("SELECT COUNT(*) AS total FROM questions")
        total_questions = cursor.fetchone()['total']
        
        try:
            cursor.execute('''
                SELECT COUNT(DISTINCT q.id) AS answered 
                FROM questions q 
                JOIN enhanced_performance ep ON q.id = ep.question_id 
                WHERE ep.correct_count > 0 OR ep.freeform_correct_count > 0 OR ep.partial_correct_count > 0
            ''')
        except:
            cursor.execute('''
                SELECT COUNT(DISTINCT q.id) AS answered 
                FROM questions q 
                JOIN enhanced_performance ep ON q.id = ep.question_id 
                WHERE ep.correct_count > 0
            ''')
        
        answered_questions = cursor.fetchone()['answered'] or 0
        mastery_value = 0
        if total_questions > 0:
            coverage = answered_questions / total_questions
            # UPDATED: Use new scoring values
            try:
                cursor.execute('''
                    SELECT q.topic, q.cefr_level,
                           COUNT(DISTINCT q.id) as questions_in_topic,
                           SUM(IFNULL(ep.correct_count, 0)) as mc_correct,
                           SUM(IFNULL(ep.incorrect_count, 0)) as mc_incorrect,
                           SUM(IFNULL(ep.freeform_correct_count, 0)) as ff_correct,
                           SUM(IFNULL(ep.freeform_incorrect_count, 0)) as ff_incorrect,
                           SUM(IFNULL(ep.partial_correct_count, 0)) as partial_correct,
                           SUM(IFNULL(ep.unanswered_count, 0)) as unanswered
                    FROM questions q
                    LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                    GROUP BY q.topic, q.cefr_level
                ''')
            except:
                # Fallback if unanswered_count doesn't exist
                cursor.execute('''
                    SELECT q.topic, q.cefr_level,
                           COUNT(DISTINCT q.id) as questions_in_topic,
                           SUM(IFNULL(ep.correct_count, 0)) as mc_correct,
                           SUM(IFNULL(ep.incorrect_count, 0)) as mc_incorrect,
                           SUM(IFNULL(ep.freeform_correct_count, 0)) as ff_correct,
                           SUM(IFNULL(ep.freeform_incorrect_count, 0)) as ff_incorrect,
                           SUM(IFNULL(ep.partial_correct_count, 0)) as partial_correct,
                           0 as unanswered
                    FROM questions q
                    LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                    GROUP BY q.topic, q.cefr_level
                ''')
            topic_data = cursor.fetchall()
            total_weighted_score = 0
            total_possible_score = 0
            for topic_row in topic_data:
                # Fixed: Handle sqlite3.Row properly - try to access the column directly
                try:
                    unanswered_count = topic_row['unanswered']
                except (IndexError, KeyError):
                    unanswered_count = 0
                    
                topic_attempts = (topic_row['mc_correct'] + topic_row['mc_incorrect'] + 
                                  topic_row['ff_correct'] + topic_row['ff_incorrect'] + 
                                  topic_row['partial_correct'] + unanswered_count)
                if topic_attempts >= 4:
                    # UPDATED SCORING VALUES
                    score = (topic_row['ff_correct'] * 2.30 +  # Changed from 1.25
                             topic_row['mc_correct'] * 0.9 + 
                             topic_row['partial_correct'] * 1.5 - 
                             topic_row['ff_incorrect'] * 2.60 -  # Changed from 1.5
                             topic_row['mc_incorrect'] * 2.80 -  # Changed from 1.75
                             unanswered_count * 0.5)  # NEW: unanswered penalty
                    max_score = topic_row['questions_in_topic'] * 1.75  # Changed from 1.25
                    total_weighted_score += max(0, score)
                    total_possible_score += max_score
            if total_possible_score > 0:
                mastery_value = (total_weighted_score / total_possible_score) * coverage
        else:
            coverage = 0
        return coverage, mastery_value
    
    def update_daily_stats(self, cursor):
        """Update daily statistics for Progress Timeline"""
        coverage, mastery = self.get_total_coverage_and_mastery(cursor)
        today = datetime.now().date()
        cursor.execute('''
            INSERT OR REPLACE INTO daily_stats (date, total_coverage, total_mastery, last_updated)
            VALUES (?, ?, ?, ?)
        ''', (today, coverage, mastery, datetime.now()))
    
    def determine_freeform_probability(self, cefr_level, freeform_mode):
        """
        Determine probability with significantly increased freeform chances
        """
        if freeform_mode == "no_freeform":
            return 0.0
        elif freeform_mode == "only_freeform":
            return 1.0
        elif freeform_mode == "adaptive":
            coverage = self.get_coverage_percentage(cefr_level)
            if coverage >= 0.75: return 1.0
            elif coverage >= 0.65: return 0.8
            elif coverage >= 0.40: return 0.6
            elif coverage >= 0.25: return 0.4
            elif coverage >= 0.20: return 0.3
            elif coverage >= 0.15: return 0.20
            elif coverage >= 0.10: return 0.12
            else: return 0.1
        return 0.0
    
    def check_freeform_answer(self, user_answer, correct_answer):
        """
        Enhanced freeform answer checking with specific rules
        NOW: Ignores trailing punctuation marks
        """
        if not user_answer or not correct_answer:
            return False, False, "No answer provided"

        # Strip trailing punctuation from both answers
        def strip_trailing_punctuation(text):
            """Remove common ending punctuation marks"""
            if not text:
                return text
            text = text.strip()
            # Remove common ending punctuation: . ? ! : ; , ...
            while text and text[-1] in '.?!:;,':
                text = text[:-1].strip()
            # Also handle ellipsis
            if text.endswith('...'):
                text = text[:-3].strip()
            return text
        
        # Clean both answers of trailing punctuation
        user_clean = strip_trailing_punctuation(user_answer)
        correct_clean = strip_trailing_punctuation(correct_answer)

        # 1. Direct match (case-insensitive, punctuation-stripped)
        if user_clean.lower().strip() == correct_clean.lower().strip():
            return True, False, None

        normalized_user = self.normalize_text_for_comparison(user_clean)
        normalized_correct = self.normalize_text_for_comparison(correct_clean)

        # 2. Normalized match (handles accents)
        if normalized_user == normalized_correct:
            return True, True, "⚠️ Correct but watch the accents!"

        # 3. Specific single NON-VOWEL mistake (add/remove/substitute)
        if self.has_single_letter_mistake(user_clean.lower().strip(), correct_clean.lower().strip()):
            return True, True, "⚠️ Correct but watch the spelling!"

        # 4. Fallback to similarity ratio, but with an explicit vowel check
        similarity = SequenceMatcher(None, normalized_user, normalized_correct).ratio()
        if similarity >= 0.85:
            if len(normalized_user) == len(normalized_correct):
                diff_indices = [i for i, (a, b) in enumerate(zip(normalized_user, normalized_correct)) if a != b]
                
                # Only if there's one single difference...
                if len(diff_indices) == 1:
                    idx = diff_indices[0]
                    vowels = {'a', 'e', 'i', 'o', 'u'}
                    
                    # ...and that difference is NOT a vowel.
                    if normalized_user[idx] not in vowels and normalized_correct[idx] not in vowels:
                        return True, True, "⚠️ Very close! Check spelling carefully."

        # If none of the above criteria are met, the answer is incorrect.
        return False, False, None

    def normalize_text_for_comparison(self, text):
        """Normalize text by removing accents and cleaning"""
        if not text: return ""
        text = text.lower().strip()
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        # Note: Don't remove ALL punctuation here since we handle trailing punctuation separately
        # This preserves internal punctuation like apostrophes in "l'amico"
        text = re.sub(r'[.!?,:;"\'\-\(\)]+$', '', text)  # Only remove trailing punctuation
        text = re.sub(r'^[.!?,:;"\'\-\(\)]+', '', text)  # And leading punctuation
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    
    def has_single_letter_mistake(self, user_text, correct_text):
        """
        Check for a single non-vowel letter mistake.
        """
        vowels = {'a', 'e', 'i', 'o', 'u'}
        if abs(len(user_text) - len(correct_text)) > 1: return False
        
        # Substitution check
        if len(user_text) == len(correct_text):
            diff_indices = [i for i, (a, b) in enumerate(zip(user_text, correct_text)) if a != b]
            if len(diff_indices) == 1:
                idx = diff_indices[0]
                # Fail if either the user's char or correct char is a vowel
                if user_text[idx] in vowels or correct_text[idx] in vowels: 
                    return False
                return True
            return False

        # Deletion check (user is missing one char)
        if len(user_text) == len(correct_text) - 1:
            for i in range(len(correct_text)):
                if user_text == correct_text[:i] + correct_text[i+1:]:
                    deleted_char = correct_text[i]
                    # Fail if the deleted character was a vowel
                    if deleted_char in vowels: 
                        return False
                    return True
            return False

        # Insertion check (user added one char)
        if len(user_text) == len(correct_text) + 1:
            for i in range(len(user_text)):
                if correct_text == user_text[:i] + user_text[i+1:]:
                    inserted_char = user_text[i]
                    # Fail if the inserted character was a vowel
                    if inserted_char in vowels: 
                        return False
                    return True
            return False
        return False
    
    def update_performance(self, cursor, question_id, cefr_level, topic, is_correct, is_freeform=False, is_partial=False, is_unanswered=False):
        """
        Update performance tracking with weighted scoring for freeform responses
        UPDATED: Now tracks unanswered questions
        """
        now = datetime.now().isoformat()
        try:
            if is_unanswered:
                # NEW: Handle unanswered questions
                cursor.execute('''
                    INSERT OR REPLACE INTO enhanced_performance (question_id, correct_count, incorrect_count, freeform_correct_count, freeform_incorrect_count, partial_correct_count, unanswered_count, last_seen, mastery_level)
                    VALUES (?, 
                            IFNULL((SELECT correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT freeform_correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT freeform_incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT partial_correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT unanswered_count FROM enhanced_performance WHERE question_id = ?), 0) + 1,
                            ?,
                            IFNULL((SELECT mastery_level FROM enhanced_performance WHERE question_id = ?), 0) - 0.5)
                ''', (question_id, question_id, question_id, question_id, question_id, question_id, question_id, now, question_id))
            elif is_freeform:
                if is_partial:
                    cursor.execute('''
                        INSERT OR REPLACE INTO enhanced_performance (question_id, correct_count, incorrect_count, freeform_correct_count, freeform_incorrect_count, partial_correct_count, unanswered_count, last_seen, mastery_level)
                        VALUES (?, 
                                IFNULL((SELECT correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT freeform_correct_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                                IFNULL((SELECT freeform_incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT partial_correct_count FROM enhanced_performance WHERE question_id = ?), 0) + 1,
                                IFNULL((SELECT unanswered_count FROM enhanced_performance WHERE question_id = ?), 0),
                                ?,
                                IFNULL((SELECT mastery_level FROM enhanced_performance WHERE question_id = ?), 0) + ?)
                    ''', (question_id, question_id, question_id, question_id, 1 if is_correct else 0, question_id, question_id, question_id, now, question_id, 0.5 if is_correct else -0.5))
                else:
                    cursor.execute('''
                        INSERT OR REPLACE INTO enhanced_performance (question_id, correct_count, incorrect_count, freeform_correct_count, freeform_incorrect_count, partial_correct_count, unanswered_count, last_seen, mastery_level)
                        VALUES (?, 
                                IFNULL((SELECT correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT freeform_correct_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                                IFNULL((SELECT freeform_incorrect_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                                IFNULL((SELECT partial_correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                                IFNULL((SELECT unanswered_count FROM enhanced_performance WHERE question_id = ?), 0),
                                ?,
                                IFNULL((SELECT mastery_level FROM enhanced_performance WHERE question_id = ?), 0) + ?)
                    ''', (question_id, question_id, question_id, question_id, 1 if is_correct else 0, question_id, 0 if is_correct else 1, question_id, question_id, now, question_id, 1.5 if is_correct else -1))
            else:
                cursor.execute('''
                    INSERT OR REPLACE INTO enhanced_performance (question_id, correct_count, incorrect_count, freeform_correct_count, freeform_incorrect_count, partial_correct_count, unanswered_count, last_seen, mastery_level)
                    VALUES (?, 
                            IFNULL((SELECT correct_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                            IFNULL((SELECT incorrect_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                            IFNULL((SELECT freeform_correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT freeform_incorrect_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT partial_correct_count FROM enhanced_performance WHERE question_id = ?), 0),
                            IFNULL((SELECT unanswered_count FROM enhanced_performance WHERE question_id = ?), 0),
                            ?,
                            IFNULL((SELECT mastery_level FROM enhanced_performance WHERE question_id = ?), 0) + ?)
                ''', (question_id, question_id, 1 if is_correct else 0, question_id, 0 if is_correct else 1, question_id, question_id, question_id, question_id, now, question_id, 1 if is_correct else -1))
        except Exception as e:
            # Fallback for databases without unanswered_count column
            cursor.execute('''
                INSERT OR REPLACE INTO enhanced_performance (question_id, correct_count, incorrect_count, last_seen, mastery_level)
                VALUES (?, 
                        IFNULL((SELECT correct_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                        IFNULL((SELECT incorrect_count FROM enhanced_performance WHERE question_id = ?), 0) + ?,
                        ?,
                        IFNULL((SELECT mastery_level FROM enhanced_performance WHERE question_id = ?), 0) + ?)
            ''', (question_id, question_id, 1 if is_correct else 0, question_id, 0 if is_correct else 1, now, question_id, 1 if is_correct else -1))
        
        weight = 1.5 if is_freeform and not is_partial else 1
        cursor.execute('''
            INSERT OR REPLACE INTO topic_performance (topic, cefr_level, correct_count, incorrect_count, last_updated)
            VALUES (?, ?, 
                    IFNULL((SELECT correct_count FROM topic_performance WHERE topic = ? AND cefr_level = ?), 0) + ?,
                    IFNULL((SELECT incorrect_count FROM topic_performance WHERE topic = ? AND cefr_level = ?), 0) + ?,
                    ?)
        ''', (topic, cefr_level, topic, cefr_level, weight if is_correct else 0, topic, cefr_level, 0 if is_correct else weight, now))

    # UPDATED: Modified to hide question counts and show only percentages
    def _calculate_mastery_for_level(self, level, cursor):
        """Helper method to calculate coverage and new mastery with updated scoring."""
        cursor.execute("SELECT COUNT(*) AS total FROM questions WHERE cefr_level = ?", (level,))
        total_q = cursor.fetchone()['total']
        if total_q == 0:
            return {"coverage": "0%", "mastery": "0%", "coverage_value": 0, "mastery_value": 0}
        
        cursor.execute('''
            SELECT COUNT(DISTINCT q.id) AS answered 
            FROM questions q JOIN enhanced_performance ep ON q.id = ep.question_id 
            WHERE q.cefr_level = ? AND (ep.correct_count > 0 OR ep.freeform_correct_count > 0 OR ep.partial_correct_count > 0)
        ''', (level,))
        answered_q = cursor.fetchone()['answered'] or 0
        coverage = (answered_q / total_q) if total_q > 0 else 0
        
        # UPDATED SCORING VALUES and added unanswered handling
        try:
            cursor.execute('''
                SELECT q.topic, COUNT(DISTINCT q.id) as questions_in_topic, 
                       SUM(IFNULL(ep.correct_count, 0)) as mc_correct,
                       SUM(IFNULL(ep.incorrect_count, 0)) as mc_incorrect,
                       SUM(IFNULL(ep.freeform_correct_count, 0)) as ff_correct,
                       SUM(IFNULL(ep.freeform_incorrect_count, 0)) as ff_incorrect,
                       SUM(IFNULL(ep.partial_correct_count, 0)) as partial_correct,
                       SUM(IFNULL(ep.unanswered_count, 0)) as unanswered
                FROM questions q LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                WHERE q.cefr_level = ? GROUP BY q.topic
            ''', (level,))
        except:
            # Fallback if unanswered_count doesn't exist
            cursor.execute('''
                SELECT q.topic, COUNT(DISTINCT q.id) as questions_in_topic,
                       SUM(IFNULL(ep.correct_count, 0)) as mc_correct,
                       SUM(IFNULL(ep.incorrect_count, 0)) as mc_incorrect,
                       SUM(IFNULL(ep.freeform_correct_count, 0)) as ff_correct,
                       SUM(IFNULL(ep.freeform_incorrect_count, 0)) as ff_incorrect,
                       SUM(IFNULL(ep.partial_correct_count, 0)) as partial_correct,
                       0 as unanswered
                FROM questions q LEFT JOIN enhanced_performance ep ON q.id = ep.question_id
                WHERE q.cefr_level = ? GROUP BY q.topic
            ''', (level,))
        
        topic_data = cursor.fetchall()
        total_weighted_score, total_possible_score = 0, 0
        
        for topic_row in topic_data:
            # Fixed: Handle sqlite3.Row properly
            try:
                unanswered_count = topic_row['unanswered']
            except (IndexError, KeyError):
                unanswered_count = 0
            
            topic_attempts = (topic_row['mc_correct'] + topic_row['mc_incorrect'] + 
                              topic_row['ff_correct'] + topic_row['ff_incorrect'] + 
                              topic_row['partial_correct'] + unanswered_count)
            
            if topic_attempts >= 4:
                # UPDATED SCORING VALUES
                score = (topic_row['ff_correct'] * 1.75 +      # Changed from 1.25
                         topic_row['mc_correct'] * 0.85 + 
                         topic_row['partial_correct'] * 1.0 - 
                         topic_row['ff_incorrect'] * 1.95 -     # Changed from 1.5
                         topic_row['mc_incorrect'] * 2.00 -     # Changed from 1.75
                         unanswered_count * 0.5)  # NEW: unanswered penalty
                max_score = topic_row['questions_in_topic'] * 1.75     # Changed from 1.25
                total_weighted_score += max(0, score)
                total_possible_score += max_score
        
        mastery = (total_weighted_score / total_possible_score) * coverage if total_possible_score > 0 else 0
        
        # UPDATED: Only show percentages, not counts
        return {
            "coverage": f"{coverage*100:.0f}%",
            "mastery": f"{mastery*100:.0f}%",
            "coverage_value": coverage,
            "mastery_value": mastery
        }

class QuizApp(ctk.CTk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title(APP_NAME)
        self.geometry(f"{WIDTH}x{HEIGHT}")
        self.resizable(True, True)
        
        self.adaptive_engine = ImprovedAdaptiveLearningEngine()
        self.current_user_level = None # To track level changes for pop-up
        
        container = ctk.CTkFrame(self)
        container.pack(side="top", fill="both", expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        
        self.frames = {}
        for F in (HomeScreen, QuizScreen, StatsScreen, TopicSelectionScreen, HowToUseScreen):
            frame = F(container, self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")
        
        self.show_frame(HomeScreen)

    def show_frame(self, cont):
        frame = self.frames[cont]
        if hasattr(frame, 'refresh_data'):
            frame.refresh_data()
        frame.tkraise()
    
    def show_level_up_popup(self, old_level, new_level):
        """Displays a congratulatory pop-up when the user's level increases."""
        dialog = ctk.CTkToplevel(self)
        dialog.title("🎉 Level Up!")
        dialog.geometry("400x200")
        dialog.transient(self)
        dialog.grab_set()
        dialog.attributes("-topmost", True)

        main_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        main_frame.pack(expand=True, fill="both", padx=20, pady=20)
        
        message_text = f"Congratulations!\n\nYou've mastered level {old_level} and have now reached:"
        
        message_label = ctk.CTkLabel(main_frame, text=message_text,
                                     font=ctk.CTkFont(size=16))
        message_label.pack(pady=(0,10))
        
        new_level_label = ctk.CTkLabel(main_frame, text=new_level,
                                     font=ctk.CTkFont(size=32, weight="bold"), text_color="#FFD700")
        new_level_label.pack(pady=10)

        ok_button = ctk.CTkButton(main_frame, text="Awesome!", command=dialog.destroy, width=120)
        ok_button.pack(pady=10)


class HomeScreen(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

        title_label = ctk.CTkLabel(self, text=APP_NAME, font=ctk.CTkFont(size=40, weight="bold"))
        title_label.pack(pady=(40, 10))

        attribution_label = ctk.CTkLabel(self, text="(V1.0.8 - Made by jvgiordano using Claude 4.0 Sonnet, Gemini 2.5 Pro, and Grok 4)", 
                                         font=ctk.CTkFont(size=14), text_color="gray60")
        attribution_label.pack(pady=(0, 20))

        # --- Main Level and Progress Display ---
        self.level_label = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=20, weight="bold"))
        self.level_label.pack(pady=(0, 5))
        
        self.working_on_label = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=16))
        self.working_on_label.pack(pady=(0, 20))

        # Frame for the three progress metrics
        self.progress_metrics_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.progress_metrics_frame.pack(pady=(0, 20))
        
        self.mastery_label = ctk.CTkLabel(self.progress_metrics_frame, text="Level Mastery: -", font=ctk.CTkFont(size=16))
        self.mastery_label.pack(side="left", padx=15)
        
        self.coverage_label = ctk.CTkLabel(self.progress_metrics_frame, text="Topic Coverage: -", font=ctk.CTkFont(size=16))
        self.coverage_label.pack(side="left", padx=15)
        
        self.streak_label = ctk.CTkLabel(self.progress_metrics_frame, text="Sustained Success: -", font=ctk.CTkFont(size=16))
        self.streak_label.pack(side="left", padx=15)
        # --- End Progress Display ---

        adaptive_quiz_button = ctk.CTkButton(self, text="Start Adaptive Quiz", 
                                             command=self.start_adaptive_quiz, 
                                             width=300, height=60, font=ctk.CTkFont(size=20))
        adaptive_quiz_button.pack(pady=20)

        freeform_label = ctk.CTkLabel(self, text="Response Type:", 
                                     font=ctk.CTkFont(size=14, weight="bold"))
        freeform_label.pack(pady=(10, 5))
        
        self.freeform_dropdown = ctk.CTkOptionMenu(self, 
                                                   values=["Adaptive Free Form Responses (Default)", 
                                                           "Only Free Form Responses", 
                                                           "No Free Form Responses (Multiple Choice Only)"],
                                                   width=400, height=35,
                                                   font=ctk.CTkFont(size=14),
                                                   dropdown_font=ctk.CTkFont(size=14),
                                                   fg_color="#334155", 
                                                   button_hover_color="#475569")
        self.freeform_dropdown.set("Adaptive Free Form Responses (Default)")
        self.freeform_dropdown.pack(pady=(0, 20))
        
        levels_frame = ctk.CTkFrame(self, fg_color="transparent")
        levels_frame.pack(pady=10)
        levels = ["A1", "A2", "B1", "B2", "C1"]
        for level in levels:
            level_button = ctk.CTkButton(levels_frame, text=f"Level {level} Quiz", 
                                         command=lambda l=level: self.start_level_quiz(l), 
                                         width=140)
            level_button.pack(side="left", padx=10)

        options_frame = ctk.CTkFrame(self, fg_color="transparent")
        options_frame.pack(pady=15)
        
        topic_button = ctk.CTkButton(options_frame, text="Quiz Select Topics", 
                                     command=lambda: controller.show_frame(TopicSelectionScreen), 
                                     width=250, height=50)
        topic_button.pack(side="left", padx=10)
        
        random_quiz_button = ctk.CTkButton(options_frame, text="Random Quiz", 
                                           command=self.start_random_quiz, 
                                           width=250, height=50)
        random_quiz_button.pack(side="left", padx=10)
        
        bottom_buttons_frame = ctk.CTkFrame(self, fg_color="transparent")
        bottom_buttons_frame.pack(pady=15)

        stats_button = ctk.CTkButton(bottom_buttons_frame, text="View Progress", 
                                     command=lambda: controller.show_frame(StatsScreen), 
                                     width=250, height=50, fg_color="#334155", hover_color="#475569")
        stats_button.pack(side="left", padx=10)

        how_to_use_button = ctk.CTkButton(bottom_buttons_frame, text="How to Use",
                                          command=lambda: controller.show_frame(HowToUseScreen),
                                          width=250, height=50, fg_color="#334155", hover_color="#475569")
        how_to_use_button.pack(side="left", padx=10)

        close_button_frame = ctk.CTkFrame(self, fg_color="transparent")
        close_button_frame.pack(side="bottom", fill="x", padx=20, pady=20)

        close_button = ctk.CTkButton(close_button_frame, text="Close Application",
                                     command=self.controller.destroy,
                                     width=150, height=40,
                                     fg_color="#D22B2B", hover_color="#AA2222")
        close_button.pack(side="right")

    def get_freeform_mode(self):
        dropdown_value = self.freeform_dropdown.get()
        if "Adaptive" in dropdown_value:
            return "adaptive"
        elif "Only Free Form" in dropdown_value:
            return "only_freeform"
        elif "No Free Form" in dropdown_value:
            return "no_freeform"
        return "adaptive"

    def start_adaptive_quiz(self, level=None):
        freeform_mode = self.get_freeform_mode()
        self.controller.frames[QuizScreen].start_quiz(adaptive=True, level=level, freeform_mode=freeform_mode)
    
    def start_level_quiz(self, level):
        freeform_mode = self.get_freeform_mode()
        self.controller.frames[QuizScreen].start_quiz(adaptive=False, level=level, freeform_mode=freeform_mode)
    
    def start_random_quiz(self):
        freeform_mode = self.get_freeform_mode()
        self.controller.frames[QuizScreen].start_quiz(adaptive=False, freeform_mode=freeform_mode)

    def refresh_data(self):
        # --- Level Up Check ---
        assessment = self.controller.adaptive_engine.assess_user_level_and_topics()
        new_level = assessment['estimated_level']
        old_level = self.controller.current_user_level
        
        # If the level has changed (and it's not the first time running)
        if old_level is not None and old_level != new_level:
            # Check if the new level is actually higher
            levels = self.controller.adaptive_engine.levels
            if levels.index(new_level) > levels.index(old_level):
                self.controller.show_level_up_popup(old_level, new_level)
        
        self.controller.current_user_level = new_level
        # --- End Level Up Check ---
        
        working_on_level = self.controller.adaptive_engine.get_next_level(new_level)

        # Update main level labels
        self.level_label.configure(text=f"Your Estimated Level: {new_level}")
        self.working_on_label.configure(text=f"Working on: {working_on_level}")

        # Get and update progress metrics for the NEXT level (the one being worked on)
        mastery_score = self.controller.adaptive_engine.get_level_mastery_score(working_on_level)
        topic_coverage = self.controller.adaptive_engine.get_level_topic_coverage(working_on_level)
        success_streak = self.controller.adaptive_engine.get_sustained_success_streak(working_on_level)

        self.mastery_label.configure(text=f"{working_on_level} Mastery: {mastery_score:.1%}")
        self.coverage_label.configure(text=f"{working_on_level} Topic Coverage: {topic_coverage:.1%}")
        self.streak_label.configure(text=f"{working_on_level} Sustained Success: {success_streak}/25")


class HowToUseScreen(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

        main_container = ctk.CTkFrame(self, fg_color="transparent")
        main_container.pack(pady=20, padx=60, fill="both", expand=True)

        title_label = ctk.CTkLabel(main_container, text="How to Use This App", font=ctk.CTkFont(size=32, weight="bold"))
        title_label.pack(pady=(20, 20))

        scrollable_frame = ctk.CTkScrollableFrame(main_container, label_text="Welcome to Progress with Italian!")
        scrollable_frame.pack(pady=10, padx=20, fill="both", expand=True)

        # UPDATED: Changed 90% to 85% in the documentation
        how_to_use_text = """
Welcome to Progress with Italian! (by jvgiordano - August, 2025)

->This program is based on "Progress with Lawless French". 

## Introduction ##

The goal of this application is to help you internalize Italian grammar so that is becomes second nature for everyday use. That is, you'll develop an intuitive grasp for the architecture of Italian grammar.

Instead of explicitly teaching the grammar rules, this program provide use-case repetition to ingrain these patterns in your brain. For example, if you answer a particular grammar question incorrectly, that specific question will be delayed from repeating. Instead, other questions targeting the same grammatical concept will be promoted and appear more frequently to help familiarize the concept. 

The goal is to learn the rule, not the answer to a specific questions.

So, how does "Progress with Italian" achieve this?

The program works by "intelligently" testing you with 10 question quizzes. This format secures a balance between creating stress and providing timely feedback, both necessary for optimized learning. 

You should always strive to get 100% on each lesson. Take the time to struggle with questions you don't know!


Even testing yourself on question you have no experience with is great for learning!


The 10 question format creates just enough psychological tension to promote focus and engagement, while the batch feedback maintains the productive stress, and avoid instant gratification. Striving for perfect scores generates beneficial stress that streghtens memory formation during learning. While short-term feedback is essential for effective learning, providing it too quickly eliminates that productive tension. The minor pause between quizzes responses and feedback also encourages retrieval practice, beneficial for memory consolidation.

An after quiz report is always provided. Slight explanations are given for wrong answers, such as the correct conjugation type. However, for rules you can't seem to grasp or aren't sticking, you may need to do additional research to gain conscious understanding to get that *click*.

## Theory of Work ##

Here's an grammatical example of how the program works (in theory):

You will be asked to fill in blanks in Italian sentences:

Question (Translation) => Answer
___ caffè non ha zucchero (My coffee doesn't have sugar.) => *Il mio* caffè non ha zucchero.
___ gatto è nero (My cat is black) => *Il mio* gatto è nero.
___ cane è piccolo (My dog is small) => *Il mio* cane è piccolo.
___ fratello è alto (My brother is tall) => *Mio* fratello è alto

You go through these examples one by one (probably not during the same quiz but over time), and begin to realize that when you want to show possesion of something (a singular masculine word here), you say "Il mio." 

Il mio caffè (My coffee)
Il mio gatto (My cat)
Il mio cane (My dog)

Pretty simple. But then you come to "fratello" (brother) and suddenly there is only "Mio". The "Il" has gone away. Huh.

You might struggle with this a bit longer. Everything seems to be "Il Mio" or "La mia" (for feminine words). Except for certain people. You might not even realize the exception is certain people because my friend (il mio amico) also requires "Il" or "la" (la mia amica). 


The general rule in Italian when showing posession is you must use an article and a possesive: 

"My cat" => "Il mio gatto". "Il" is the article, "mio" is the possessive. Literally, "The my cat."


The exception to this is singular family members: 

"My brother" => "Mio fratello"
 

This program stives to teach you these rules without explaining them, but sometimes, a little conscious knowledge can make the unconscious *click*. 

Hopefully, the hints provided after the quizzes should help if you get stuck. But should you need a little conscious enlightenment, you might need to do some research (try an LLM - Gemini, Claude, Grok, etc.)


The difference now besides teacing you the lesson explicitly is your brain has already realized there is a rule there, somewhere, somehow related to this, it just wasn't able to tease the arbritrariness out. 


## Depth, Difficulty, and Progression ##


Concerning the depth and difficulty: 

There are 5 levels of Italian provided, based on the CEFR framework. A1 and A2 are for beginner, B1 and B2 are intermediate, and C1 is advanced. To progress between levels, you need to satisfy ALL THREE criteria:
• **Sustained Success:** Achieve an average of 85% correct answers in the last 50 questions, and sustain this for 25 consecutive additional questions from that level.
• **Broad Coverage:** Attempt at least 85% of the available topics within that level (only one question per topic).
• **Minimum Mastery:** Attain a mastery score of at least 50% for the level. (Mastery scoring explained below).


All new beginners actually start at A0 (there are no A0 questions.)


THIS PROGRAM AIMS TO BE CONSERVATIVE IN ITS CEFR ESTIMATION. DO NOT BE DISCOURAGED.


The system also allows non-sequential progression - if you start practicing B1 and demonstrate mastery, you'll be recognized as B1 even without completing A1/A2. You can also select to only cover grammar or vocabulary topics which interest you, and you can even select specific topics across CEFR levels you want to practice (like present tense conjugations and futuro semplice).

The program has two response formats: Multiple Choice and Free Form (fill in). By default, the program uses a mix of these. As you advance, more questions will become free-form, including those you may have previously answered as multiple choice. But the program offers other response modes.

The app includes three response modes:
• **Adaptive Free Form Responses (Default):** Questions become free form (type your answer) based on your mastery level. Higher coverage = more free form questions. This prevents memorization and increases challenge as you improve.
• **Only Free Form Responses:** All questions require typing the correct answer.
• **No Free Form Responses:** Traditional multiple choice only.

Mastery Scoring is separate from coverage scoring. Mastery scoring aims to demonstrate how well you have "mastered"  a level.

Free form answers are worth more points! The system uses intelligent matching with the following rules:
• **Accents:** Not required but recommended (marked as partial if missing)
• **Spelling:** Single non-vowel letter mistakes are accepted (marked as partial)
• **Case:** Not case-sensitive
• **Punctuation:** Ignored in grading
• **Articles:** must be included if part of the answer

Examples of Partial Credit:
✓ "caffe" instead of "caffè" (missing accent)
✓ "gato" instead of "gatto" (single consonant difference is allowed)
✗ "bella" instead of "bello" (vowel difference is not allowed)
✗ "cafe" instead of "caffè" (multiple errors)

Response modes provide different points towards this score:
• Free-form correct: +1.75 points
• Multiple choice correct: +0.85 points
• Free-form (partially correct): +1.0 points
• Unanswered questions: -0.5 points
• Free-form wrong: -1.95 points
• Multiple choice wrong: -2.00 points
• Topics must be seen 4+ times to count toward mastery (that is 4 questions from a given topic)


There is an "Adaptive" algorithm which adjust the difficulty and level of questions based on your usage and compentency.

The "Adaptive" algorithm gives you 5-9 questions per quiz from your next CEFR level estimate, with the number decreasing as you approach mastery. So, A0 level users (newbies) will mostly get A1 questions. 

For C1 users (advanced), this changes to 3-6 questions to ensure a broader review of previous levels. The remainder of the quiz is filled with questions from nearby levels.

The Progress section will show your progress in each section. The CEFR bar chart will show you coverage and mastery. The blue bars represent coverage (questions you've encountered once), and the gold bars show your mastery (weighted by difficulty and topic progress). You can clear all your progress here as well.

There will be some errors in the questions. I've done my best to remove these, but if you have any doubts use a translator or LLM to double-check. Let me know of any problems and I will update the program as needed.

For those with American keyboards, I recommend installing the "United States - International" keyboard on Windows for typing with accents.

Good luck!


"""
        text_label = ctk.CTkLabel(scrollable_frame, text=how_to_use_text,
                                 font=ctk.CTkFont(size=16),
                                 justify="left",
                                 wraplength=WIDTH - 280)
        text_label.pack(pady=10, padx=20)

        back_button = ctk.CTkButton(main_container, text="Back to Start Page",
                                    command=lambda: controller.show_frame(HomeScreen),
                                    height=40)
        back_button.pack(pady=20)

class TopicSelectionScreen(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.topic_vars = []

        title_label = ctk.CTkLabel(self, text="Select Topics to Practice", font=ctk.CTkFont(size=28, weight="bold"))
        title_label.pack(pady=20)

        self.scrollable_frame = ctk.CTkScrollableFrame(self)
        self.scrollable_frame.pack(pady=10, padx=20, fill="both", expand=True)

        control_frame = ctk.CTkFrame(self)
        control_frame.pack(pady=10, padx=20, fill="x")
        
        start_button = ctk.CTkButton(control_frame, text="Start Quiz with Selected Topics", command=self.start_topic_quiz)
        start_button.pack(side="right")

        back_button = ctk.CTkButton(control_frame, text="Back to Home", command=lambda: controller.show_frame(HomeScreen))
        back_button.pack(side="left")

    def refresh_data(self):
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.topic_vars.clear()

        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cefr_level, topic FROM questions ORDER BY cefr_level, topic")
        all_topics = cursor.fetchall()
        conn.close()

        grouped_topics = {}
        for row in all_topics:
            if row['cefr_level'] not in grouped_topics:
                grouped_topics[row['cefr_level']] = []
            grouped_topics[row['cefr_level']].append(row['topic'])

        for level, topics in grouped_topics.items():
            level_label = ctk.CTkLabel(self.scrollable_frame, text=f"Level {level}", font=ctk.CTkFont(size=18, weight="bold"))
            level_label.pack(anchor="w", padx=10, pady=(15, 5))
            for topic in topics:
                var = ctk.StringVar(value="off")
                cb = ctk.CTkCheckBox(self.scrollable_frame, text=topic, variable=var, onvalue=topic, offvalue="off")
                cb.pack(anchor="w", padx=20, pady=2)
                self.topic_vars.append(var)
    
    def start_topic_quiz(self):
        selected_topics = [var.get() for var in self.topic_vars if var.get() != "off"]
        if selected_topics:
            freeform_mode = self.controller.frames[HomeScreen].get_freeform_mode()
            self.controller.frames[QuizScreen].start_quiz(adaptive=False, topics=selected_topics, freeform_mode=freeform_mode)

class QuizScreen(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.quiz_items = []
        self.quiz_session_id = str(uuid.uuid4())
        
        self.scrollable_frame = ctk.CTkScrollableFrame(self, label_text="Italian Quiz")
        self.scrollable_frame.pack(pady=20, padx=20, fill="both", expand=True)
        
        # Configure grid for centering with max width
        self.scrollable_frame.grid_columnconfigure(0, weight=1, minsize=100)  # Left margin
        self.scrollable_frame.grid_columnconfigure(1, weight=0, minsize=800, uniform="content")  # Content column with max width
        self.scrollable_frame.grid_columnconfigure(2, weight=1, minsize=100)  # Right margin
        
        self.control_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.control_frame.pack(pady=10, padx=20, fill="x")
        
        self.submit_all_button = ctk.CTkButton(self.control_frame, text="Submit All Answers", command=self.submit_all_answers, height=40)
        self.quit_button = ctk.CTkButton(self.control_frame, text="Quit and Go to Start Page", command=lambda: controller.show_frame(HomeScreen), height=40, fg_color="#D22B2B", hover_color="#AA2222")
        self.back_button = ctk.CTkButton(self.control_frame, text="Back to Home", command=lambda: controller.show_frame(HomeScreen), height=40)

    def start_quiz(self, adaptive=True, level=None, topics=None, freeform_mode="adaptive"):
        self.quiz_session_id = str(uuid.uuid4())
        self.freeform_mode = freeform_mode
        
        if adaptive:
            self.questions = self.controller.adaptive_engine.fetch_adaptive_questions(
                level=level, topics=topics
            )
        else:
            conn = sqlite3.connect('italian_quiz.db')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # FIXED: Added hint column to the query
            query = '''SELECT id, question_text, english_translation, option_a, option_b, 
                           option_c, option_d, correct_option, cefr_level, topic, explanation, resource,
                           hint 
                     FROM questions'''
            params = []
            where_clauses = []
            
            if level:
                where_clauses.append("cefr_level = ?")
                params.append(level)
            if topics:
                placeholders = ','.join('?' for _ in topics)
                where_clauses.append(f"topic IN ({placeholders})")
                params.extend(topics)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            query += " ORDER BY RANDOM() LIMIT ?"
            params.append(10)
            
            cursor.execute(query, params)
            self.questions = [dict(q) for q in cursor.fetchall()]
            conn.close()

        if not self.questions:
            error_dialog = ctk.CTkToplevel(self)
            error_dialog.title("No Questions Found")
            error_dialog.geometry("300x150")
            error_dialog.transient(self.controller)
            error_dialog.grab_set()
            
            error_text = "No questions found for the selected criteria."
            if level:
                error_text += f"\nLevel: {level}"
            if topics:
                error_text += f"\nTopics: {', '.join(topics)}"
            
            error_label = ctk.CTkLabel(error_dialog, text=error_text, font=ctk.CTkFont(size=14))
            error_label.pack(pady=20)
            
            ok_button = ctk.CTkButton(error_dialog, text="OK", command=error_dialog.destroy)
            ok_button.pack(pady=10)
            
            return
        
        title_text = "Italian Quiz"
        if adaptive and not level and not topics:
            title_text += " - Adaptive"
        elif level:
            title_text += f" - Level {level}"
        if topics:
            if len(topics) == 1:
                title_text += f" - {topics[0]}"
            else:
                title_text += f" - {len(topics)} Topics Selected"
        
        if freeform_mode == "only_freeform":
            title_text += " (Free Form Only)"
        elif freeform_mode == "no_freeform":
            title_text += " (Multiple Choice Only)"
        elif freeform_mode == "adaptive":
            title_text += " (Adaptive Free Form)"
        
        self.scrollable_frame.configure(label_text=title_text)
        self.populate_quiz_frame()
        self.scrollable_frame._parent_canvas.yview_moveto(0)  # Reset scroll to top
        self.controller.show_frame(QuizScreen)

    def populate_quiz_frame(self):
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        self.quiz_items.clear()
        
        self.back_button.pack_forget()
        self.submit_all_button.pack(side="right")
        self.quit_button.pack(side="left")

        # Create a centered container with max width
        self.scrollable_frame.grid_columnconfigure(0, weight=1)
        self.scrollable_frame.grid_columnconfigure(2, weight=1)
        
        current_row = 0
        for i, q_data in enumerate(self.questions):
            # FIXED: Force freeform for questions starting with "Write:"
            if q_data['question_text'].strip().startswith("Write:"):
                is_freeform = True
            else:
                freeform_probability = self.controller.adaptive_engine.determine_freeform_probability(q_data['cefr_level'], self.freeform_mode)
                is_freeform = random.random() < freeform_probability
            
            question_container = ctk.CTkFrame(self.scrollable_frame, fg_color="#F8F9FA", corner_radius=10)
            question_container.grid(row=current_row, column=1, sticky="ew", pady=(10, 5), padx=20)
            # Set maximum width for the container
            question_container.grid_configure(ipadx=0, ipady=0)
            self.scrollable_frame.grid_rowconfigure(current_row, weight=0)
            
            question_row = ctk.CTkFrame(question_container, fg_color="transparent")
            question_row.pack(fill="x", padx=20, pady=(15, 10))
            
            number_label = ctk.CTkLabel(question_row, text=f"{i+1}.", 
                                        font=ctk.CTkFont(size=26, weight="bold"), 
                                        text_color="#2B2B2B", width=40)
            number_label.pack(side="left", padx=(0, 10), anchor="n")
            
            text_container = ctk.CTkFrame(question_row, fg_color="transparent")
            text_container.pack(side="left", fill="both", expand=True)
            
            question_text = q_data['question_text']
            english_text = q_data['english_translation']
            
            has_translation = english_text and not q_data['question_text'].startswith("How would you say")
            
            # FIXED: Use hint from database column instead of extracting from question text
            hint_text = q_data.get('hint', '')
            
            # Still check for old-style hints in parentheses for backward compatibility
            if not hint_text and "(" in question_text and question_text.rstrip().endswith(")"):
                last_paren = question_text.rfind("(")
                if last_paren != -1:
                    hint_text = question_text[last_paren:]
                    question_text = question_text[:last_paren].strip()
            
            input_widget = None
            
            if is_freeform:
                if "___" in q_data['question_text'] and q_data['question_text'].count("___") == 1:
                    integrated_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    integrated_frame.pack(anchor="w")
                    
                    parts = question_text.split("___")
                    
                    if len(parts) == 2:
                        if parts[0].strip():
                            before_label = ctk.CTkLabel(integrated_frame, text=parts[0], 
                                                        font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                            before_label.pack(side="left")
                        
                        input_widget = ctk.CTkEntry(integrated_frame, width=200, height=35,
                                                    font=ctk.CTkFont(size=18),
                                                    placeholder_text="Type your answer...")
                        input_widget.pack(side="left", padx=(10, 10))
                        
                        if parts[1].strip():
                            after_label = ctk.CTkLabel(integrated_frame, text=parts[1], 
                                                       font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                            after_label.pack(side="left")
                        
                        if has_translation:
                            formatted_translation = f"({english_text})"
                            english_label = ctk.CTkLabel(integrated_frame, text=formatted_translation,
                                                         font=ctk.CTkFont(size=16, slant="italic"),
                                                         text_color="#555555",
                                                         justify="left")
                            english_label.pack(side="left", padx=(5, 0))
                    else:
                        q_and_t_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                        q_and_t_frame.pack(anchor="w", pady=(0, 5))

                        question_label = ctk.CTkLabel(q_and_t_frame, text=question_text, 
                                                     font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                        question_label.pack(side="left")

                        if has_translation:
                            formatted_translation = f"({english_text})"
                            english_label = ctk.CTkLabel(q_and_t_frame, text=formatted_translation,
                                                         font=ctk.CTkFont(size=16, slant="italic"),
                                                         text_color="#555555",
                                                         justify="left")
                            english_label.pack(side="left", padx=(5, 0))
                        
                        input_widget = ctk.CTkEntry(text_container, width=400, height=35,
                                                    font=ctk.CTkFont(size=18),
                                                    placeholder_text="Type your answer...")
                        input_widget.pack(anchor="w", pady=5)
                        
                elif question_text.startswith("How would you say") or ":" in question_text:
                    integrated_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    integrated_frame.pack(anchor="w")
                    
                    question_label = ctk.CTkLabel(integrated_frame, text=question_text + " ", 
                                                 font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                    question_label.pack(side="left")
                    
                    input_widget = ctk.CTkEntry(integrated_frame, width=200, height=35,
                                                font=ctk.CTkFont(size=18),
                                                placeholder_text="Type answer...")
                    input_widget.pack(side="left", padx=(10, 0))
                else:
                    q_and_t_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    q_and_t_frame.pack(anchor="w", pady=(0, 5))
                    
                    question_label = ctk.CTkLabel(q_and_t_frame, text=question_text, 
                                                 font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                    question_label.pack(side="left")

                    if has_translation:
                        formatted_translation = f"({english_text})"
                        english_label = ctk.CTkLabel(q_and_t_frame, text=formatted_translation,
                                                     font=ctk.CTkFont(size=16, slant="italic"),
                                                     text_color="#555555",
                                                     justify="left")
                        english_label.pack(side="left", padx=(5, 0))
                    
                    input_widget = ctk.CTkEntry(text_container, width=400, height=35,
                                                font=ctk.CTkFont(size=18),
                                                placeholder_text="Type your answer...")
                    input_widget.pack(anchor="w", pady=5)
                         
            else:
                option_values = [
                    q_data['option_a'],
                    q_data['option_b'],
                    q_data['option_c'],
                    q_data['option_d']
                ]
                
                random.shuffle(option_values)
                
                if "___" in q_data['question_text'] and q_data['question_text'].count("___") == 1:
                    integrated_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    integrated_frame.pack(anchor="w")
                    
                    parts = question_text.split("___")
                    
                    if len(parts) == 2:
                        if parts[0].strip():
                            before_label = ctk.CTkLabel(integrated_frame, text=parts[0], 
                                                        font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                            before_label.pack(side="left")
                        
                        input_widget = ctk.CTkOptionMenu(integrated_frame, values=option_values, 
                                                         width=300, height=35,
                                                         font=ctk.CTkFont(size=18),
                                                         dropdown_font=ctk.CTkFont(size=18))
                        input_widget.set("Choose answer...")
                        input_widget.pack(side="left", padx=(10, 10))
                        
                        if parts[1].strip():
                            after_label = ctk.CTkLabel(integrated_frame, text=parts[1], 
                                                       font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                            after_label.pack(side="left")

                        if has_translation:
                            formatted_translation = f"({english_text})"
                            english_label = ctk.CTkLabel(integrated_frame, text=formatted_translation,
                                                         font=ctk.CTkFont(size=16, slant="italic"),
                                                         text_color="#555555",
                                                         justify="left")
                            english_label.pack(side="left", padx=(5, 0))
                    else:
                        q_and_t_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                        q_and_t_frame.pack(anchor="w", pady=(0, 5))
                        
                        question_label = ctk.CTkLabel(q_and_t_frame, text=question_text, 
                                                     font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                        question_label.pack(side="left")

                        if has_translation:
                            formatted_translation = f"({english_text})"
                            english_label = ctk.CTkLabel(q_and_t_frame, text=formatted_translation,
                                                         font=ctk.CTkFont(size=16, slant="italic"),
                                                         text_color="#555555",
                                                         justify="left")
                            english_label.pack(side="left", padx=(5, 0))
                        
                        input_widget = ctk.CTkOptionMenu(text_container, values=option_values, 
                                                         width=400, height=35,
                                                         font=ctk.CTkFont(size=18),
                                                         dropdown_font=ctk.CTkFont(size=18))
                        input_widget.set("Choose your answer...")
                        input_widget.pack(anchor="w", pady=5)
                        
                elif question_text.startswith("How would you say") or ":" in question_text:
                    integrated_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    integrated_frame.pack(anchor="w")
                    
                    question_label = ctk.CTkLabel(integrated_frame, text=question_text + " ", 
                                                 font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                    question_label.pack(side="left")
                    
                    input_widget = ctk.CTkOptionMenu(integrated_frame, values=option_values, 
                                                     width=300, height=35,
                                                     font=ctk.CTkFont(size=18),
                                                     dropdown_font=ctk.CTkFont(size=18))
                    input_widget.set("Choose answer...")
                    input_widget.pack(side="left", padx=(10, 0))
                else:
                    q_and_t_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                    q_and_t_frame.pack(anchor="w", pady=(0, 5))
                    
                    question_label = ctk.CTkLabel(q_and_t_frame, text=question_text, 
                                                 font=ctk.CTkFont(size=18), text_color="#1a1a1a")
                    question_label.pack(side="left")

                    if has_translation:
                        formatted_translation = f"({english_text})"
                        english_label = ctk.CTkLabel(q_and_t_frame, text=formatted_translation,
                                                     font=ctk.CTkFont(size=16, slant="italic"),
                                                     text_color="#555555",
                                                     justify="left")
                        english_label.pack(side="left", padx=(5, 0))
                    
                    input_widget = ctk.CTkOptionMenu(text_container, values=option_values, 
                                                     width=400, height=35,
                                                     font=ctk.CTkFont(size=18),
                                                     dropdown_font=ctk.CTkFont(size=18))
                    input_widget.set("Choose your answer...")
                    input_widget.pack(anchor="w", pady=5)
            
            # Display hint if it exists
            has_hint = hint_text and hint_text.strip()
            
            if has_hint:
                info_frame = ctk.CTkFrame(text_container, fg_color="transparent")
                info_frame.pack(anchor="w", pady=5)

                # Clean up hint text - remove parentheses if present
                cleaned_hint = hint_text.strip()
                if cleaned_hint.startswith('(') and cleaned_hint.endswith(')'):
                    cleaned_hint = cleaned_hint[1:-1]
                
                hint_label = ctk.CTkLabel(info_frame, text=f"HINT: {cleaned_hint}",
                                          font=ctk.CTkFont(size=16),
                                          text_color="#666666",
                                          justify="left")
                hint_label.pack(anchor="w", pady=(5, 0))

            # FIXED: Remove CEFR level display during quiz to prevent cheating
            # This section has been removed - no more info_label showing cefr_level
            
            current_row += 1

            if i < len(self.questions) - 1:
                separator = ctk.CTkFrame(self.scrollable_frame, height=2, fg_color="#E0E0E0")
                separator.grid(row=current_row, column=1, sticky="ew", padx=40, pady=(15, 5))
                current_row += 1

            self.quiz_items.append({
                "question_id": q_data['id'],
                "correct_option": q_data['correct_option'],
                "cefr_level": q_data['cefr_level'],
                "topic": q_data['topic'],
                "input_widget": input_widget,
                "explanation": q_data['explanation'],
                "question_data": q_data,
                "is_freeform": is_freeform
            })
            
        self.scrollable_frame._parent_canvas.yview_moveto(0)

    def submit_all_answers(self):
        correct_answers = []
        incorrect_answers = []
        partial_answers = []
        unanswered_questions = []  # NEW: Track unanswered questions
        
        conn = None
        try:
            conn = sqlite3.connect('italian_quiz.db')
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()

            for item in self.quiz_items:
                user_answer_text = ""
                is_unanswered = False  # NEW: Flag for unanswered questions
                
                if item["is_freeform"]:
                    user_answer_text = item["input_widget"].get().strip()
                    if not user_answer_text:
                        user_answer_text = "No answer provided"
                        is_unanswered = True
                else:
                    user_answer_text = item["input_widget"].get()
                    if "Choose" in user_answer_text or "Select" in user_answer_text:
                        user_answer_text = "No answer selected"
                        is_unanswered = True

                q_data = item['question_data']
                correct_option_letter = item["correct_option"]
                correct_answer_text = q_data[f'option_{correct_option_letter.lower()}']
                
                is_correct, is_partial, partial_feedback = False, False, None
                
                if is_unanswered:
                    # NEW: Handle unanswered questions
                    is_correct = False
                    is_partial = False
                elif item["is_freeform"]:
                    is_correct, is_partial, partial_feedback = self.controller.adaptive_engine.check_freeform_answer(user_answer_text, correct_answer_text)
                    
                    # If not correct, check against alternate responses
                    if not is_correct and q_data.get('alternate_correct_responses'):
                        alt_responses = q_data['alternate_correct_responses'].split(';')
                        for alt_answer in alt_responses:
                            alt_answer = alt_answer.strip()
                            if alt_answer:
                                alt_correct, alt_partial, alt_feedback = self.controller.adaptive_engine.check_freeform_answer(user_answer_text, alt_answer)
                                if alt_correct:
                                    is_correct = alt_correct
                                    is_partial = alt_partial
                                    partial_feedback = alt_feedback
                                    break
                
                else:
                    is_correct = (user_answer_text == correct_answer_text)
                
                result_item = {
                    'item': item,
                    'user_answer_full': user_answer_text,
                    'is_correct': is_correct,
                    'is_partial': is_partial,
                    'partial_feedback': partial_feedback,
                    'question_data': q_data,
                    'is_unanswered': is_unanswered  # NEW: Track if unanswered
                }
                
                if is_unanswered:
                    unanswered_questions.append(result_item)  # NEW: Add to unanswered list
                elif is_correct and is_partial:
                    partial_answers.append(result_item)
                elif is_correct:
                    correct_answers.append(result_item)
                else:
                    incorrect_answers.append(result_item)
                
                # UPDATED: Pass is_unanswered flag to update_performance
                self.controller.adaptive_engine.update_performance(
                    cursor, item["question_id"], item["cefr_level"], item["topic"], 
                    is_correct, item["is_freeform"], is_partial, is_unanswered
                )

                # Log the individual answer for recency tracking
                # A partial answer is still a success for the 85% rule
                # NEW: Unanswered questions count as failures
                is_success_for_recency = 1 if (is_correct or is_partial) else 0
                cursor.execute(
                    "INSERT INTO answer_history (question_id, cefr_level, is_correct, timestamp) VALUES (?, ?, ?, ?)",
                    (item["question_id"], item["cefr_level"], is_success_for_recency, datetime.now().isoformat())
                )
            
            score = len(correct_answers) + len(partial_answers)
            total = len(self.quiz_items)
            if total > 0:
                now = datetime.now().isoformat()
                cursor.execute(
                    "INSERT INTO quiz_history (session_id, score, total_questions, timestamp) VALUES (?, ?, ?, ?)",
                    (self.quiz_session_id, score, total, now)
                )

            self.controller.adaptive_engine.update_daily_stats(cursor)
            conn.commit()

        except sqlite3.Error as e:
            print(f"Database error during quiz submission: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        
        self.submit_all_button.pack_forget()
        self.quit_button.pack_forget()
        self.back_button.pack(side="right")
        
        self.scrollable_frame._parent_canvas.yview_moveto(0)
        
        # UPDATED: Pass unanswered questions to show_enhanced_results
        self.show_enhanced_results(incorrect_answers, partial_answers, correct_answers, unanswered_questions)


    def show_enhanced_results(self, incorrect_answers, partial_answers, correct_answers, unanswered_questions):
        """Show results with wrong answers at top, partial in middle, correct at bottom
        UPDATED: Now includes unanswered questions section"""
        
        # Setup centered grid layout
        self.scrollable_frame.grid_columnconfigure(0, weight=1)
        self.scrollable_frame.grid_columnconfigure(2, weight=1)
        
        title_label = ctk.CTkLabel(self.scrollable_frame, text="📊 Quiz Results", font=ctk.CTkFont(size=28, weight="bold"))
        title_label.grid(row=0, column=1, pady=(0, 20), sticky="ew")
        
        current_row = self.show_final_score(len(correct_answers) + len(partial_answers), len(self.quiz_items), 1)
        
        # NEW: Unanswered Questions Section
        if unanswered_questions:
            unanswered_title_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#696969", corner_radius=8)
            unanswered_title_frame.grid(row=current_row, column=1, sticky="ew", pady=(10, 10), padx=20)
            current_row += 1
            
            unanswered_title = ctk.CTkLabel(unanswered_title_frame, text=f"⏭️ Unanswered Questions ({len(unanswered_questions)})", 
                                           font=ctk.CTkFont(size=18, weight="bold"), text_color="white")
            unanswered_title.pack(pady=10)
            
            for i, result in enumerate(unanswered_questions):
                current_row = self.display_answer_feedback(result, current_row, feedback_type="unanswered")
        
        # Incorrect Answers Section
        if incorrect_answers:
            wrong_title_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#8B0000", corner_radius=8)
            wrong_title_frame.grid(row=current_row, column=1, sticky="ew", pady=(10, 10), padx=20)
            current_row += 1
            
            wrong_title = ctk.CTkLabel(wrong_title_frame, text=f"❌ Incorrect Answers ({len(incorrect_answers)})", 
                                       font=ctk.CTkFont(size=18, weight="bold"), text_color="white")
            wrong_title.pack(pady=10)
            
            for i, result in enumerate(incorrect_answers):
                current_row = self.display_answer_feedback(result, current_row, feedback_type="incorrect")
        
        # Partially Correct Answers Section
        if partial_answers:
            partial_title_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#FF8C00", corner_radius=8)
            partial_title_frame.grid(row=current_row, column=1, sticky="ew", pady=(10, 10), padx=20)
            current_row += 1
            
            partial_title = ctk.CTkLabel(partial_title_frame, text=f"⚠️ Partially Correct ({len(partial_answers)})", 
                                         font=ctk.CTkFont(size=18, weight="bold"), text_color="white")
            partial_title.pack(pady=10)
            
            for i, result in enumerate(partial_answers):
                current_row = self.display_answer_feedback(result, current_row, feedback_type="partial")
        
        # Correct Answers Section
        if correct_answers:
            correct_title_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#006400", corner_radius=8)
            correct_title_frame.grid(row=current_row, column=1, sticky="ew", pady=(20, 10), padx=20)
            current_row += 1
            
            correct_title = ctk.CTkLabel(correct_title_frame, text=f"✅ Correct Answers ({len(correct_answers)})", 
                                         font=ctk.CTkFont(size=18, weight="bold"), text_color="white")
            correct_title.pack(pady=10)
            
            for i, result in enumerate(correct_answers):
                current_row = self.display_answer_feedback(result, current_row, feedback_type="correct")

    def display_answer_feedback(self, result, row, feedback_type):
        """IMPROVED: Display individual answer feedback with better legibility and layout
        UPDATED: Now handles 'unanswered' feedback type"""
        item = result['item']
        user_answer_full = result['user_answer_full']
        is_correct = result['is_correct']
        is_partial = result.get('is_partial', False)
        partial_feedback = result.get('partial_feedback', None)
        q_data = result['question_data']
        is_unanswered = result.get('is_unanswered', False)  # NEW
        
        question_number = None
        for i, quiz_item in enumerate(self.quiz_items):
            if quiz_item['question_id'] == item['question_id']:
                question_number = i + 1
                break
        
        feedback_container = ctk.CTkFrame(self.scrollable_frame, fg_color="#F8F9FA", corner_radius=10)
        feedback_container.grid(row=row, column=1, sticky="ew", pady=(10, 5), padx=20)
        
        question_row = ctk.CTkFrame(feedback_container, fg_color="transparent")
        question_row.pack(fill="x", padx=20, pady=(15, 10))
        
        if question_number:
            number_label = ctk.CTkLabel(question_row, text=f"{question_number}.", 
                                        font=ctk.CTkFont(size=18), 
                                        text_color="#2B2B2B", width=30)
            number_label.pack(side="left", padx=(0, 10))
        
        # IMPROVED: Better layout for question content
        text_container = ctk.CTkFrame(question_row, fg_color="transparent")
        text_container.pack(side="left", fill="x", expand=True)
        
        question_text = q_data['question_text']
        english_text = q_data['english_translation']
        
        question_display = question_text
        if english_text and not q_data['question_text'].startswith("How would you say"):
            question_display += f"    ({english_text})"
        
        question_label = ctk.CTkLabel(text_container, text=question_display, 
                                     font=ctk.CTkFont(size=18), 
                                     text_color="#1a1a1a", wraplength=WIDTH-200, 
                                     justify="left")
        question_label.pack(anchor="w", pady=(0, 5))
                
        # KEEP: Show CEFR level and topic in results (not hidden like in quiz)
        info_label = ctk.CTkLabel(question_row, text=f"{q_data['cefr_level']} | {q_data['topic']}", 
                                  font=ctk.CTkFont(size=14), text_color="#666666")
        info_label.pack(side="right", padx=(20, 0))
        
        # IMPROVED: Better answer section layout with improved colors and readability
        answer_section = ctk.CTkFrame(feedback_container, fg_color="transparent")
        answer_section.pack(fill="x", padx=20, pady=(0, 15))
        
        # Create a dedicated answers container with more space
        answers_container = ctk.CTkFrame(answer_section, fg_color="#FFFFFF", corner_radius=8)
        answers_container.pack(fill="x", padx=10, pady=5)
        
        # Add spacing from the left
        spacing_frame = ctk.CTkFrame(answers_container, fg_color="transparent")
        spacing_frame.pack(fill="x", padx=15, pady=10)
        
        # Add Resource button
        button_frame = ctk.CTkFrame(spacing_frame, fg_color="transparent")
        button_frame.pack(fill="x", pady=(0, 10))
        
        if q_data.get('resource'):
            resource_button = ctk.CTkButton(button_frame, text="Resource", 
                                          command=lambda url=q_data['resource']: webbrowser.open(url),
                                          width=100, height=30,
                                          fg_color="#1F6AA5", hover_color="#1854A0")
            resource_button.pack(side="right")
        
        # UPDATED: Handle different feedback types including unanswered
        if feedback_type == "unanswered":
            user_answer_label = ctk.CTkLabel(spacing_frame, 
                                             text="⏭️ Question was not answered", 
                                             font=ctk.CTkFont(size=16, weight="bold"), 
                                             text_color="#696969")  # Gray color for unanswered
            user_answer_label.pack(anchor="w", pady=(2, 0))
        elif feedback_type == "partial":
            user_answer_label = ctk.CTkLabel(spacing_frame, 
                                             text=f"Your Answer: {user_answer_full}", 
                                             font=ctk.CTkFont(size=16, weight="bold"), 
                                             text_color="#E65100")  # Darker orange for better contrast
            user_answer_label.pack(anchor="w", pady=(2, 0))
            
            if partial_feedback:
                partial_label = ctk.CTkLabel(spacing_frame, 
                                             text=partial_feedback, 
                                             font=ctk.CTkFont(size=15, weight="bold"), 
                                             text_color="#FF8F00")  # Medium orange
                partial_label.pack(anchor="w", pady=(2, 0))
        elif feedback_type == "incorrect":
            user_answer_label = ctk.CTkLabel(spacing_frame, 
                                             text=f"Your Answer: {user_answer_full}", 
                                             font=ctk.CTkFont(size=16, weight="bold"), 
                                             text_color="#C62828")  # Darker red for better contrast
            user_answer_label.pack(anchor="w", pady=(2, 0))
        
        correct_option = item["correct_option"]
        correct_option_text = q_data[f'option_{correct_option.lower()}']
        correct_label = ctk.CTkLabel(spacing_frame, 
                                     text=f"Correct Answer: {correct_option_text}", 
                                     font=ctk.CTkFont(size=16, weight="bold"), 
                                     text_color="#2E7D32")  # Darker green for better contrast
        correct_label.pack(anchor="w", pady=(2, 0))
        
        # IMPROVED: Better explanation layout with proper text wrapping
        if item["explanation"]:
            # Add some visual separation
            separator_frame = ctk.CTkFrame(spacing_frame, height=1, fg_color="#E0E0E0")
            separator_frame.pack(fill="x", pady=(8, 8))
            
            explanation_label = ctk.CTkLabel(spacing_frame, 
                                             text=f"Explanation: {item['explanation']}", 
                                             text_color="#424242",
                                             font=ctk.CTkFont(size=14, weight="bold"), 
                                             wraplength=WIDTH-200,  # REDUCED from 120 to 200 to account for all container padding
                                             justify="left")
            explanation_label.pack(anchor="w", pady=(0, 5), padx=(10, 10), fill="x")  # ADDED padx for left/right padding
        
        return row + 1

    def show_final_score(self, score, total, row):
        """Display final score at the top of the results"""
        percentage = (score / total) * 100 if total > 0 else 0
        
        score_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#2B2B2B", corner_radius=10)
        score_frame.grid(row=row, column=1, sticky="ew", pady=(0, 20), padx=20)
        
        score_text = f"{score}/{total} ({percentage:.1f}%)"
        score_label = ctk.CTkLabel(score_frame, text=score_text, 
                                   font=ctk.CTkFont(size=28, weight="bold"), 
                                   text_color="#4CAF50" if percentage >= 80 else "#FF9800" if percentage >= 60 else "#F44336")
        score_label.pack(pady=(10, 5))
        
        if percentage >= 90:
            message = "🎉 Eccellente! Outstanding performance!"
        elif percentage >= 80:
            message = "👏 Molto bene! Great job!"
        elif percentage >= 70:
            message = "👍 Bene! Good work, keep practicing!"
        elif percentage >= 60:
            message = "📚 Non male! Not bad, room for improvement."
        else:
            message = "💪 Keep studying! Practice makes perfect."
            
        message_label = ctk.CTkLabel(score_frame, text=message, 
                                     font=ctk.CTkFont(size=14), text_color="gray70")
        message_label.pack(pady=(0, 15))
        return row + 1

class StatsScreen(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        
        header_frame = ctk.CTkFrame(self, fg_color="transparent")
        header_frame.pack(fill="x", padx=20, pady=20)
        
        back_button = ctk.CTkButton(header_frame, text="← Back to Home", 
                                    command=lambda: controller.show_frame(HomeScreen),
                                    width=120, height=32)
        back_button.pack(side="left")
        
        title_label = ctk.CTkLabel(header_frame, text="📈 Progress and Analytics", font=ctk.CTkFont(size=28, weight="bold"))
        title_label.pack(side="left", padx=(40, 0))
        # NOTE: Clear button removed from header

        self.main_scroll = ctk.CTkScrollableFrame(self)
        self.main_scroll.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # New progress summary frame
        self.summary_frame = ctk.CTkFrame(self.main_scroll, fg_color="transparent")
        self.summary_frame.pack(fill="x", pady=(0, 20), padx=50)
        self.summary_label = ctk.CTkLabel(self.summary_frame, text="", font=ctk.CTkFont(size=18))
        self.summary_label.pack(pady=15, padx=10)

        # Progress Timeline (new top graph)
        self.timeline_frame = ctk.CTkFrame(self.main_scroll)
        self.timeline_frame.pack(pady=(0, 20), padx=50)
        
        # CEFR Level Completion and Level Progress table
        self.stats_frame = ctk.CTkFrame(self.main_scroll)
        self.stats_frame.pack(pady=(0, 20), padx=50)
        
        # NEW: Detailed level-by-level progress metrics
        self.level_details_frame = ctk.CTkFrame(self.main_scroll)
        self.level_details_frame.pack(fill="x", pady=(0, 20), padx=50)
        
        # Recent Quiz Performance (moved down)
        self.graph_frame = ctk.CTkFrame(self.main_scroll)
        self.graph_frame.pack(pady=(0, 20), padx=50)
        
        self.weaknesses_frame = ctk.CTkFrame(self.main_scroll)
        self.weaknesses_frame.pack(fill="x", pady=(0, 20), padx=50)
        
        self.explanation_frame = ctk.CTkFrame(self.main_scroll)
        self.explanation_frame.pack(fill="x", pady=(0, 20), padx=50)
        
        # Clear Progress button at the very bottom
        clear_button_frame = ctk.CTkFrame(self.main_scroll, fg_color="transparent")
        clear_button_frame.pack(fill="x", pady=(40, 40), padx=50)
        
        clear_button = ctk.CTkButton(clear_button_frame, text="Clear All Progress",
                                     command=self.confirm_clear_progress,
                                     fg_color="#D22B2B", hover_color="#AA2222",
                                     width=200, height=40)
        clear_button.pack()

    def refresh_data(self):
        self.update_summary()
        self.update_progress_timeline()
        self.update_stats_table()
        self.update_level_details()
        self.update_graph()
        self.update_weaknesses()
        self.update_explanations()
        # Scroll to top after refreshing all data
        self.main_scroll._parent_canvas.yview_moveto(0)

    def update_summary(self):
        """Updates the new summary text at the top of the stats page."""
        current_level = self.controller.adaptive_engine.calculate_estimated_level()
        next_level = self.controller.adaptive_engine.get_next_level(current_level)
        
        summary_text = f"Estimated CEFR Level: {current_level}    |    You are working towards: {next_level}"
        if current_level == next_level: # Max level reached
            summary_text = f"Estimated CEFR Level: {current_level}    |    Congratulations, you've reached the highest level!"

        self.summary_label.configure(text=summary_text)

    def update_progress_timeline(self):
        """Create the new Progress Timeline plot"""
        for widget in self.timeline_frame.winfo_children():
            widget.destroy()
        
        title_label = ctk.CTkLabel(self.timeline_frame, text="Progress Timeline", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 10))
        
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        
        # Get all daily stats
        cursor.execute('''
            SELECT date, total_coverage, total_mastery 
            FROM daily_stats 
            ORDER BY date
        ''')
        daily_data = cursor.fetchall()
        
        # Get the first quiz date if no daily stats exist yet
        if not daily_data:
            cursor.execute('''
                SELECT MIN(DATE(timestamp)) as first_date 
                FROM quiz_history
            ''')
            first_quiz = cursor.fetchone()
            if first_quiz and first_quiz[0]:
                # Temporarily get a row_factory cursor to calculate current stats
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                coverage, mastery = self.controller.adaptive_engine.get_total_coverage_and_mastery(cursor)
                daily_data = [(datetime.now().date().isoformat(), coverage, mastery)]
        
        conn.close()
        
        if not daily_data:
            no_data_label = ctk.CTkLabel(self.timeline_frame, 
                                         text="No progress data yet.\nComplete a quiz to start tracking your progress!", 
                                         font=ctk.CTkFont(size=14))
            no_data_label.pack(expand=True, pady=30)
            return
        
        # Parse dates and values
        dates = []
        coverage_values = []
        mastery_values = []
        
        # Add a starting point (day before first data)
        first_date = datetime.fromisoformat(daily_data[0][0])
        start_date = first_date - timedelta(days=1)
        dates.append(start_date)
        coverage_values.append(0)
        mastery_values.append(0)
        
        # Add actual data
        for date_str, coverage, mastery in daily_data:
            dates.append(datetime.fromisoformat(date_str))
            coverage_values.append(coverage * 100)  # Convert to percentage
            mastery_values.append(mastery * 100)
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(10, 5), facecolor="#F0F0F0")
        
        # Plot coverage line with blue fill
        ax.fill_between(dates, 0, coverage_values, alpha=0.5, color='#1F6AA5', zorder=1)
        ax.plot(dates, coverage_values, color='#1F6AA5', linewidth=2, zorder=2)
        
        # Plot mastery line with gold fill (on top)
        ax.fill_between(dates, 0, mastery_values, alpha=0.7, color='#FFD700', zorder=3)
        ax.plot(dates, mastery_values, color='#FFD700', linewidth=2, zorder=4)
        
        # Format the plot
        ax.set_xlabel("Date", color="black", fontsize=11)
        ax.set_ylabel("Percentage (%)", color="black", fontsize=11)
        
        # Autoscale y-axis with some padding
        if coverage_values or mastery_values:
            max_value = max(max(coverage_values) if coverage_values else 0, 
                           max(mastery_values) if mastery_values else 0)
            min_value = min(min(coverage_values) if coverage_values else 0,
                           min(mastery_values) if mastery_values else 0)
            
            # Add 10% padding above and below, but keep 0 as minimum if data starts from 0
            padding = (max_value - min_value) * 0.1 if max_value > min_value else 5
            y_min = max(0, min_value - padding)  # Don't go below 0
            y_max = min(105, max_value + padding)  # Don't exceed 105% (since it's percentage)
            
            # If values are very small, ensure we have a reasonable scale
            if y_max < 10:
                y_max = 10
                
            ax.set_ylim(y_min, y_max)
        else:
            ax.set_ylim(0, 10)  # Default for no data
        
        ax.set_facecolor("#FFFFFF")
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
        fig.autofmt_xdate()
        
        # Grid
        ax.grid(True, which='major', linestyle='--', linewidth=0.5, color='gray', alpha=0.3)
        
        # Spines
        for spine in ax.spines.values():
            spine.set_color('black')
            spine.set_linewidth(1)
        
        ax.tick_params(axis='x', colors='black')
        ax.tick_params(axis='y', colors='black')
        
        # Create horizontal legend above the plot
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#1F6AA5', alpha=0.7, label='Coverage'),
            Patch(facecolor='#FFD700', alpha=0.7, label='Mastery')
        ]
        ax.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, 1.15),
                  ncol=2, frameon=False)
        
        canvas = FigureCanvasTkAgg(fig, master=self.timeline_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(side="top", fill="both", expand=True, pady=10)


    def _calculate_mastery_for_level(self, level, cursor):
        """Helper method to calculate coverage and new mastery with updated scoring."""
        return self.controller.adaptive_engine._calculate_mastery_for_level(level, cursor)

    def update_stats_table(self):
        for widget in self.stats_frame.winfo_children():
            widget.destroy()
        
        second_row_container = ctk.CTkFrame(self.stats_frame, fg_color="transparent")
        second_row_container.pack(fill="both", expand=True, pady=15)
        
        cefr_frame = ctk.CTkFrame(second_row_container)
        cefr_frame.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        cefr_title_label = ctk.CTkLabel(cefr_frame, text="CEFR Level Completion", 
                                        font=ctk.CTkFont(size=18, weight="bold"))
        cefr_title_label.pack(pady=(15, 10))
        
        self.create_cefr_completion_chart(cefr_frame)
        
        table_container = ctk.CTkFrame(second_row_container)
        table_container.pack(side="right", fill="both", expand=True, padx=(10, 0))
        
        title_label = ctk.CTkLabel(table_container, text="Level Progress", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 20))
        
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        table_frame = ctk.CTkFrame(table_container)
        table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        for i in range(3):
            table_frame.grid_columnconfigure(i, weight=1)
        
        cursor.execute("SELECT DISTINCT cefr_level FROM questions ORDER BY cefr_level")
        levels = [row['cefr_level'] for row in cursor.fetchall()]
        
        stats = {}
        for level in levels:
            stats[level] = self._calculate_mastery_for_level(level, cursor)
        
        conn.close()

        headers = ["Level", "Coverage", "Mastery"]
        for i, header in enumerate(headers):
            header_label = ctk.CTkLabel(table_frame, text=header, font=ctk.CTkFont(size=14, weight="bold"))
            header_label.grid(row=0, column=i, padx=5, pady=5, sticky="nsew")
        
        for row_num, (level, data) in enumerate(stats.items(), start=1):
            ctk.CTkLabel(table_frame, text=level, font=ctk.CTkFont(size=14)).grid(
                row=row_num, column=0, padx=5, pady=2, sticky="nsew")
            ctk.CTkLabel(table_frame, text=data['coverage'], font=ctk.CTkFont(size=14)).grid(
                row=row_num, column=1, padx=5, pady=2, sticky="nsew")
            ctk.CTkLabel(table_frame, text=data['mastery'], font=ctk.CTkFont(size=14)).grid(
                row=row_num, column=2, padx=5, pady=2, sticky="nsew")

    def update_level_details(self):
        """NEW: Displays detailed Mastery, Coverage, and Success Streak for each level."""
        for widget in self.level_details_frame.winfo_children():
            widget.destroy()

        title_label = ctk.CTkLabel(self.level_details_frame, text="Detailed Level Progress",
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 10))
        
        levels = self.controller.adaptive_engine.levels
        
        details_container = ctk.CTkFrame(self.level_details_frame, fg_color="transparent")
        details_container.pack(pady=(0, 15))

        for level in levels:
            if level == 'A0':
                continue

            mastery_score = self.controller.adaptive_engine.get_level_mastery_score(level)
            topic_coverage = self.controller.adaptive_engine.get_level_topic_coverage(level)
            success_streak = self.controller.adaptive_engine.get_sustained_success_streak(level)
            
            row_frame = ctk.CTkFrame(details_container, fg_color="transparent")
            row_frame.pack(fill="x", expand=True, pady=5)
            
            level_label = ctk.CTkLabel(row_frame, text=f"{level}:", font=ctk.CTkFont(size=16, weight="bold"), width=40)
            level_label.pack(side="left", padx=(0, 15))
            
            metrics_frame = ctk.CTkFrame(row_frame, fg_color="transparent")
            metrics_frame.pack(side="left", expand=True)

            mastery_label = ctk.CTkLabel(metrics_frame, text=f"Mastery: {mastery_score:.1%}", font=ctk.CTkFont(size=16))
            mastery_label.pack(side="left", padx=15)
            
            coverage_label = ctk.CTkLabel(metrics_frame, text=f"Topic Coverage: {topic_coverage:.1%}", font=ctk.CTkFont(size=16))
            coverage_label.pack(side="left", padx=15)
            
            streak_label = ctk.CTkLabel(metrics_frame, text=f"Sustained Success: {success_streak}/25", font=ctk.CTkFont(size=16))
            streak_label.pack(side="left", padx=15)

    def update_graph(self):
        for widget in self.graph_frame.winfo_children():
            widget.destroy()
        
        title_label1 = ctk.CTkLabel(self.graph_frame, text="Recent Quiz Performance", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label1.pack(pady=(15, 10))
        
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT score, total_questions FROM quiz_history ORDER BY timestamp DESC LIMIT 20")
            history = cursor.fetchall()
        except sqlite3.OperationalError:
            history = []
        conn.close()
        
        history.reverse()

        if not history:
            no_data_label = ctk.CTkLabel(self.graph_frame, text="No quiz history to display.\nComplete a quiz to see your progress!", 
                                         font=ctk.CTkFont(size=14))
            no_data_label.pack(expand=True, pady=30)
        else:
            scores = [(score / total) * 100 for score, total in history if total > 0]
            quiz_numbers = list(range(1, len(scores) + 1))
            
            recent_average = sum(scores) / len(scores) if scores else 0
            
            fig1, ax1 = plt.subplots(figsize=(10, 4), facecolor="#F0F0F0")
            ax1.plot(quiz_numbers, scores, marker='o', linestyle='-', color='#1F6AA5', linewidth=2, markersize=6)
            
            ax1.set_xlabel("Quiz Session", color="black", fontsize=11)
            ax1.set_ylabel("Score (%)", color="black", fontsize=11)
            ax1.tick_params(axis='x', colors='black')
            ax1.tick_params(axis='y', colors='black')
            ax1.set_facecolor("#FFFFFF")
            
            for spine in ax1.spines.values():
                spine.set_color('black')
                spine.set_linewidth(1)
            
            ax1.set_ylim(0, 105)
            ax1.grid(True, which='major', linestyle='--', linewidth=0.5, color='gray', alpha=0.7)
            
            if len(quiz_numbers) > 1:
                ax1.set_xticks(quiz_numbers)
            
            ax1.axhline(y=recent_average, color='red', linestyle='--', alpha=0.7, linewidth=2, 
                        label=f'Average: {recent_average:.1f}%')
            ax1.legend()

            canvas1 = FigureCanvasTkAgg(fig1, master=self.graph_frame)
            canvas1.draw()
            canvas1.get_tk_widget().pack(side="top", fill="both", expand=True, pady=10)
    
    def create_cefr_completion_chart(self, parent_frame):
        """Create the CEFR Level Completion bar chart with updated legend."""
        conn = sqlite3.connect('italian_quiz.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        levels = ['A1', 'A2', 'B1', 'B2', 'C1']
        coverage_data = []
        mastery_data = []
        
        for level in levels:
            stats = self._calculate_mastery_for_level(level, cursor)
            coverage_data.append(stats['coverage_value'] * 100)
            mastery_data.append(stats['mastery_value'] * 100)
        
        conn.close()
        
        fig2, ax2 = plt.subplots(figsize=(6, 4), facecolor="#F0F0F0")
        
        x_pos = range(len(levels))
        bar_width = 0.6
        
        bars_coverage = ax2.bar(x_pos, coverage_data, bar_width, label='Coverage', 
                                color='#1F6AA5', alpha=0.8, zorder=1)
        bars_mastery = ax2.bar(x_pos, mastery_data, bar_width, label='Mastery', 
                               color='#FFD700', alpha=0.9, zorder=2)
        
        ax2.set_xlabel("CEFR Level", color="black", fontsize=11)
        ax2.set_ylabel("Completion (%)", color="black", fontsize=11)
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(levels)
        ax2.set_ylim(0, 105)
        ax2.set_yticks([25, 50, 75, 100])
        
        ax2.set_facecolor("#FFFFFF")
        
        for y_val in [25, 50, 75, 100]:
            ax2.axhline(y=y_val, color='gray', linestyle='-', linewidth=0.5, alpha=0.3, zorder=0)
        
        for spine in ax2.spines.values():
            spine.set_color('black')
            spine.set_linewidth(1)
        
        ax2.tick_params(axis='x', colors='black')
        ax2.tick_params(axis='y', colors='black')
        
        ax2.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=2, frameon=False)
        
        canvas2 = FigureCanvasTkAgg(fig2, master=parent_frame)
        canvas2.draw()
        canvas2.get_tk_widget().pack(side="top", fill="both", expand=True, pady=10)

    def update_weaknesses(self):
        for widget in self.weaknesses_frame.winfo_children():
            widget.destroy()
        
        title_label = ctk.CTkLabel(self.weaknesses_frame, text="Areas for Improvement", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 15))
        
        assessment = self.controller.adaptive_engine.assess_user_level_and_topics()
        weaknesses = assessment['topic_weaknesses']
        
        if not weaknesses:
            no_weakness_label = ctk.CTkLabel(self.weaknesses_frame, 
                                             text="Great job! No significant weaknesses detected.\nKeep practicing to maintain your skills!", 
                                             font=ctk.CTkFont(size=14))
            no_weakness_label.pack(pady=20)
            return
        
        scrollable_weaknesses = ctk.CTkScrollableFrame(self.weaknesses_frame, height=150)
        scrollable_weaknesses.pack(fill="both", expand=True, pady=(0, 15))
        
        for weakness in weaknesses[:10]:
            weakness_frame = ctk.CTkFrame(scrollable_weaknesses, fg_color="transparent")
            weakness_frame.pack(fill="x", pady=5)
            
            topic_label = ctk.CTkLabel(weakness_frame, 
                                       text=f"{weakness['level']} - {weakness['topic']}: {weakness['success_rate']*100:.0f}% success rate", 
                                       font=ctk.CTkFont(size=14))
            topic_label.pack(side="left")
            
            practice_button = ctk.CTkButton(weakness_frame, text="Practice Now", 
                                            command=lambda l=weakness['level'], t=weakness['topic']: self.practice_topic(t, l),
                                            width=100, height=28)
            practice_button.pack(side="right")
    
    def practice_topic(self, topic, level):
        """Start a quiz focused on a specific topic and level."""
        freeform_mode = self.controller.frames[HomeScreen].get_freeform_mode()
        self.controller.frames[QuizScreen].start_quiz(adaptive=False, level=level, topics=[topic], freeform_mode=freeform_mode)

    def update_explanations(self):
        """Add explanation box for metrics - UPDATED with new scoring values"""
        for widget in self.explanation_frame.winfo_children():
            widget.destroy()
        
        title_label = ctk.CTkLabel(self.explanation_frame, text="Metrics Explained", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 15))
        
        scrollable_explanations = ctk.CTkScrollableFrame(self.explanation_frame)
        scrollable_explanations.pack(fill="both", expand=True, pady=(0, 15))
        
        coverage_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        coverage_frame.pack(fill="x", pady=(0, 10))
        
        coverage_title = ctk.CTkLabel(coverage_frame, text="Coverage", 
                                     font=ctk.CTkFont(size=14, weight="bold"), text_color="#1F6AA5")
        coverage_title.pack(anchor="w")
        
        coverage_text = ctk.CTkLabel(coverage_frame, 
                                     text="Shows how many questions you've answered correctly at least once. This represents the breadth of your exposure to the material. Blue bars in the CEFR chart represent this metric.",
                                     font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        coverage_text.pack(anchor="w", pady=(2, 0))
        
        mastery_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        mastery_frame.pack(fill="x", pady=(10, 10))
        
        mastery_title = ctk.CTkLabel(mastery_frame, text="🏆 Mastery", 
                                     font=ctk.CTkFont(size=14, weight="bold"), text_color="#FFD700")
        mastery_title.pack(anchor="w")
        
        mastery_text = ctk.CTkLabel(mastery_frame, 
                                    text="Calculated from your Coverage and weighted performance. Topics must be attempted at least 4 times to count toward mastery. Free-form answers are worth more than multiple choice, with penalties for incorrect answers.",
                                    font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        mastery_text.pack(anchor="w", pady=(2, 0))
        
        adaptive_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        adaptive_frame.pack(fill="x", pady=(10, 10))
        
        adaptive_title = ctk.CTkLabel(adaptive_frame, text="🧠 Adaptive Learning", 
                                     font=ctk.CTkFont(size=14, weight="bold"), text_color="#9C27B0")
        adaptive_title.pack(anchor="w")
        
        adaptive_text = ctk.CTkLabel(adaptive_frame, 
                                     text="The system ensures 60-70% of questions match your level, with the remainder from nearby levels. It avoids recently seen questions (especially wrong ones) and focuses on weak topics to promote actual learning rather than memorization.",
                                     font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        adaptive_text.pack(anchor="w", pady=(2, 0))
        
        requirements_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        requirements_frame.pack(fill="x", pady=(10, 10))
        
        requirements_title = ctk.CTkLabel(requirements_frame, text="📋 Scoring System", 
                                         font=ctk.CTkFont(size=14, weight="bold"), text_color="#FF5722")
        requirements_title.pack(anchor="w")
        
        # UPDATED: Show new scoring values
        requirements_text = ctk.CTkLabel(requirements_frame, 
                                         text="• Free-form correct: +1.75 points\n• Multiple choice correct: +0.85 points\n• Partially correct (free-form): +1.0 points\n• Unanswered questions: -0.5 points\n• Free-form wrong: -1.95 points\n• Multiple choice wrong: -2.00 points\n• Topics must be seen 4+ times to count toward mastery",
                                         font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        requirements_text.pack(anchor="w", pady=(2, 0))
        
        progress_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        progress_frame.pack(fill="x", pady=(10, 0))
        
        progress_title = ctk.CTkLabel(progress_frame, text="📈 Progress Timeline", 
                                      font=ctk.CTkFont(size=14, weight="bold"), text_color="#4CAF50")
        progress_title.pack(anchor="w")
        
        progress_text = ctk.CTkLabel(progress_frame, 
                                     text="The timeline shows your daily progress. Blue area shows topic coverage (questions seen), gold area shows mastery (weighted performance). Your goal is to reach 100% coverage, then have mastery catch up and cover it.",
                                     font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        progress_text.pack(anchor="w", pady=(2, 0))
        
        level_up_frame = ctk.CTkFrame(scrollable_explanations, fg_color="transparent")
        level_up_frame.pack(fill="x", pady=(10, 0))

        level_up_title = ctk.CTkLabel(level_up_frame, text="🚀 Level Advancement",
                                      font=ctk.CTkFont(size=14, weight="bold"), text_color="#2196F3")
        level_up_title.pack(anchor="w")

        # UPDATED: Changed from 90% to 85% for topic coverage
        level_up_text = ctk.CTkLabel(level_up_frame,
                                     text="To advance a CEFR level, you must meet three conditions: 1. Sustained success (85% correct over 50 questions, for 25 consecutive windows). 2. Broad topic coverage (85% of topics in that level attempted). 3. A minimum overall mastery score of 50%.",
                                     font=ctk.CTkFont(size=12), wraplength=400, justify="left")
        level_up_text.pack(anchor="w", pady=(2, 0))

    def confirm_clear_progress(self):
        dialog = ctk.CTkToplevel(self)
        dialog.title("Confirm Clear Progress")
        dialog.geometry("400x200")
        dialog.transient(self.controller)
        dialog.grab_set()

        label = ctk.CTkLabel(dialog, text="Are you sure you want to delete ALL your progress?\nThis action cannot be undone.",
                             font=ctk.CTkFont(size=16), wraplength=350)
        label.pack(pady=20, padx=20)

        button_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        button_frame.pack(pady=20)

        def do_clear():
            self.clear_all_progress()
            dialog.destroy()

        confirm_btn = ctk.CTkButton(button_frame, text="Yes, Clear Progress",
                                    command=do_clear,
                                    fg_color="#D22B2B", hover_color="#AA2222")
        confirm_btn.pack(side="left", padx=10)

        cancel_btn = ctk.CTkButton(button_frame, text="Cancel", command=dialog.destroy)
        cancel_btn.pack(side="left", padx=10)

    def clear_all_progress(self):
        conn = sqlite3.connect('italian_quiz.db')
        cursor = conn.cursor()
        
        tables_to_clear = [
            'enhanced_performance',
            'topic_performance',
            'quiz_history',
            'daily_stats',
            'answer_history'
        ]
        
        try:
            for table in tables_to_clear:
                cursor.execute(f"DELETE FROM {table};")
            conn.commit()
        except Exception as e:
            print(f"Error clearing progress: {e}")
        finally:
            conn.close()
        
        self.refresh_data()
        self.controller.frames[HomeScreen].refresh_data()


if __name__ == "__main__":
    app = QuizApp()
    app.mainloop()