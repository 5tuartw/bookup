import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from redis import Redis, RedisError
from rq import Queue
import json
import os
import requests
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
import sqlite3
import math
from collections import Counter

analyser = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm")
custom_stop_words = ['book', 'novel', 'story', 'page', 'read', 'author', 'world', 'new', 'man', 'woman', 'time']
all_stop_words = STOP_WORDS.union(custom_stop_words)

def analyse_review(review_text):
    print(f"Processing review in background: {review_text}")
    vs = analyser.polarity_scores(review_text)
    compound_score = vs['compound']

    if compound_score >= 0.05:
        sentiment = "Positive"
    elif compound_score <= -0.05:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    time.sleep(5)
    result = f"Analysis complete for: {review_text}. Sentiment: {sentiment} (Compound Score: {compound_score:.2f})"
    print(result)
    return result

def find_books_via_google_search(user_book_titles):
    """Searches Google Books API for potential matches for user-entered titles."""
    results_list = []
    print(f"Starting Google Books search for: {user_book_titles}")

    for user_title in user_book_titles:
        user_title_processed = user_title.strip()
        if not user_title_processed:
            continue

        possible_matches = []
        print(f" - Searching for: '{user_title_processed}'")
        search_url = f"https://www.googleapis.com/books/v1/volumes?q=intitle:{requests.utils.quote(user_title_processed)}&langRestrict=en&maxResults=5&projection-lite"
        
        try:
            response = requests.get(search_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("totalItems", 0) > 0 and "items" in data:
                for item in data["items"]:
                    volume_info = item.get("volumeInfo", {})
                    title = volume_info.get("title")
                    authors = volume_info.get("authors", ["Unknown Author"])
                    isbn13 = None
                    isbn10 = None

                    identifiers = volume_info.get("industryIdentifiers", [])
                    for identifier in identifiers:
                        if identifier.get("type") == "ISBN_13":
                            isbn13 = identifier.get("identifier")
                        elif identifier.get("type") == "ISBN_10":
                            isbn10 = identifier.get("identifier")
                    
                    if isbn13 or isbn10:
                        match_data = {
                            "match": {
                                "title": title,
                                "authors": authors,
                                "isbn": isbn13 if isbn13 else isbn10
                            },
                        }
                        possible_matches.append(match_data)

        except requests.exceptions.RequestException as e:
            print(f"   - Error searching Google Books for '{user_title_processed}': {e}")
        except Exception as e:
            print(f"   - Unexpected error processing results for '{user_title_processed}': {e}")
        
        results_list.append({
            "user_title": user_title,
            "possible_matches": possible_matches
        })

    print("Finish Google Books search.")
    return {"results_per_title": results_list}


def extract_keywords_from_text(text):
    keywords = []
    doc = nlp(text.lower())

    for token in doc:
        if (token.pos_ in ['NOUN', 'ADJ'] and
            token.lemma_ not in all_stop_words and
            not token.is_punct and
            token.pos_ != 'PROPN' and
            len(token.lemma_) > 2):
            keywords.append(token.lemma_)
    
    return keywords

def get_llm_analysis_for_book_local(book_data, redis_conn):
    isbn = book_data.get('isbn')
    title = book_data.get('title')

    authors_list = []
    authors_data = book_data.get('authors') # Could be list or JSON string

    if isinstance(authors_data, list):
        authors_list = authors_data
    elif isinstance(authors_data, str):
        if authors_data.strip():
            try:
                parsed_authors = json.loads(authors_data)
                if isinstance(parsed_authors, list):
                    authors_list = parsed_authors
                else:
                    print(f"Warning: Parsed authors JSON string for ISBN {isbn} was not a list: {parsed_authors}")
                    authors_list = [] 
            except json.JSONDecodeError:
                print(f"Warning: Could not parse authors as JSON for ISBN {isbn}. Input: '{authors_data}'")
                authors_list = []
        else: 
            authors_list = []
    elif authors_data is None:
         authors_list = []
    else:
         print(f"Warning: Unexpected type for authors field for ISBN {isbn}: {type(authors_data)}")
         authors_list = []

    author = authors_list[0] if authors_list else 'Unknown Author'

    if not isbn or not title:
        print("Warning: Missing ISBN or Title, cannot cache or analyse.")
        return None
    
    cache_key = f"llm_cache:{isbn}"
    ollama_url = "http://localhost:11434/api/generate"
    model_name = "llama3.1:8b"
    llm_results = None

    try:
        cached_data = redis_conn.get(cache_key)
        if cached_data:
            print(f"Cache HIT for ISBN {isbn}")
            try:
                llm_results = json.loads(cached_data)
                return llm_results
            except json.JSONDecodeError:
                print(f"Warning: Could not parse cached JSON for {isbn}. Fetching fresh.")
        
        print(f"Cache MISS for ISBN {isbn}. Calling local Ollama API ({model_name})...")

        prompt = (
            f"Analyse the book '{title} by {author}. Based on public knowledge and common reader discussions, please provide:\n"
            f"1. genre: List the primary genre(s). These should be broad genres so that similar stories can be easily grouped together, so avoid compound genre titles \
                (e.g., 'science fiction, 'historical fantasy', 'thriller').\n"
            f"2. setting_period: State the primary time period \
                (e.g., 'contemporary', 'Victorian era', 'futuristic')\n"
            f"3. setting_location: Describe the primary location or type of setting \
                (e.g., 'London, England', 'Small town USA', 'space station', 'fictional kingdom')\n"
            f"4. tone: List up to three primary tones (e.g. 'humourous', 'suspenseful', 'bleak', 'nostalgic', 'satirical')\n"
            f"5. target_audience: State the primary target audience \
                (e.g. 'children', 'young adult')\n"
            f"6. themes: a list of the top 5-7 recurring themes or key aspects frequently discussed by readers, in single words or commonly used two-word phrases (e.g., 'vanity', 'hedonism', 'freedom') \n"
            f"7. sentiment: A brief (one-sentence) summary of the overall reader sentiment towards the book \
                (e.g., beloved classic, controversial, thought-provoking, enjoyable adventure, etc.).\n"
            f"Format the output ONLY as a valid JSON object with these exact keys: \
                'genre': (list of strings), \
                'setting_period': (string), \
                'setting_location': (string), \
                'tone': (list of strings), \
                'target_audience': string, \
                'themes': (a list of strings) and \
                'sentiment': (a string). \
                Do not include any text outside of the JSON object. The output will be used to categorise and compare books, so all of the data should be broad enough to allow for this."
        )

        payload = {
            "model": model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json"
        }
        headers = {'Content-Type': 'application/json'}

        response = requests.post(ollama_url, headers=headers, data=json.dumps(payload), timeout=300)
        response.raise_for_status()

        ollama_response_data = response.json()
        llm_output_str = ollama_response_data.get("response", "")

        if not llm_output_str:
            print(f"Warning: Empty response content from Ollama for {isbn}")
            return None
        
        try:
            llm_results = json.loads(llm_output_str)
            expected_keys = {'genre', 'setting_period', 'setting_location', 'tone', 'target_audience', 'themes', 'sentiment'}
            if isinstance(llm_results, dict) and expected_keys.issubset(llm_results.keys()):
                if not isinstance(llm_results.get('genre'), list): llm_results['genre'] = [str(llm_results.get('genre'))]
                if not isinstance(llm_results.get('tone'), list): llm_results['tone'] = [str(llm_results.get('tone'))]
                if not isinstance(llm_results.get('themes'), list): llm_results['themes'] = [str(llm_results.get('themes'))]

                redis_conn.set(cache_key, llm_output_str)
                # redis_conn.expire(cache_key, 3600 * 24 * 30) # expire in 30 days
                print(f"Stored LLM response in cache for ISBN {isbn}")
            else:
                print(f"Warning: LLM response for {isbn} lacked expected keys. Parse: {llm_results}")
                llm_results = None
        except json.JSONDecodeError:
            print(f"Error: LLM output for {isbn} was not valid json despite requesting JSON format.")
            print(f"Raw output string was: {llm_output_str}")
            llm_results = None
    
    except RedisError as e:
        print(f"Redis error for key {cache_key}: {e}. Cannot use cache.")
        llm_results = None
    except requests.exceptions.RequestException as e:
        print(f"Error calling local Ollama API at {ollama_url}: {e}")
        print("Is the Ollama service running?")
        llm_results = None
    except Exception as e:
        print(f"An unexpected error occured in get_llm_analysis_for_book_local for {isbn}: {e}")
        llm_results = None
    
    return llm_results

# --- Gathers LLM derived data in books.db as well as Google Books API data to make profile of user's preferences ---
# Weights books based on their Google Books API averageRating and ratingCount data
# Raw weighted counts stored for later use in calculate_similarity function
def generate_user_profile(analysed_user_books):
    """
    Processes data from the user's analysed books to create a preference profile

    Args:
        analysed_user_books (dict): A dictionary where keys are ISBNs and values are
                                    book detail dicts (including LLM analysis fields 
                                    like 'llm_genre', 'llm_tone', 'llm_setting_period').
    
    Returns:
        dict: a user profile dictionary.
    """
    if not analysed_user_books or not isinstance(analysed_user_books, dict):
        print("Warning: generate_user_profile received invalid or empty input.")
        return {}
    
    feature_aggregation = {
        'genre': Counter(),
        'tone': Counter(),
        'theme': Counter(),
        'setting_period': Counter(),
        'setting_location': Counter(),
        'target_audience': Counter()
    }
    all_authors = []
    read_isbns = list(analysed_user_books.keys())
    
    total_google_rating_points = 0
    total_google_ratings_count_for_avg = 0
    books_with_google_rating_data = 0

    for isbn, book_details in analysed_user_books.items():
        if not book_details or not isinstance(book_details, dict):
            continue

        book_weight = 1.0
        avg_rating = book_details.get('averageRating')
        ratings_count = book_details.get('ratingsCount')

        if avg_rating is not None and ratings_count is not None:
            try:
                r = float(avg_rating)
                c = int(ratings_count)
                if r > 0 and c > 0:
                    # ensure that rating is between 1 and 5
                    clamped_rating = max(1, min(r, 5))
                    # use logarithmic scale to valance ratings counts for books of vastly differing popularity
                    # examples:
                    #   log10(10 + 1) ≈ 1.04
                    #   log10(100 + 1) ≈ 2.00
                    #   log10(1000 + 1) ≈ 3.00
                    #   log10(100000 + 1) ≈ 5.00
                    book_weight = clamped_rating * math.log10(c+1) # +1 for books that might have 0 ratings

                    total_google_rating_points += r * c
                    total_google_ratings_count_for_avg += c
                    books_with_google_rating_data += 1
            except (ValueError, TypeError):
                book_weight = 1.0

        def aggregate_list_feature(items, feature_name):
            if items and isinstance(items, list):
                for item in items:
                    if item and isinstance(item, str):
                        feature_aggregation[feature_name][item.lower()] += book_weight
        
        def aggregate_string_feature(item_value, feature_name):
            if item_value and isinstance(item_value, str):
                feature_aggregation[feature_name][item_value.lower()] += book_weight
        
        aggregate_list_feature(book_details.get('llm_genre'), 'genre')
        aggregate_list_feature(book_details.get('llm_tone'), 'tone')
        aggregate_list_feature(book_details.get('llm_themes'), 'theme')

        aggregate_string_feature(book_details.get('llm_setting_period'),'setting_period')
        aggregate_string_feature(book_details.get('llm_setting_location'),'setting_location')
        aggregate_string_feature(book_details.get('llm_target_audience'),'target_audience')

        authors_list = book_details.get('authors', [])
        if isinstance(authors_list, list):
            all_authors.extend(a.lower() for a in authors_list if a and isinstance(a, str))

    user_profile = {
        'top_genres': [item[0] for item in  feature_aggregation['genre'].most_common(3)],
        'top_tones': [item[0] for item in  feature_aggregation['tone'].most_common(3)],
        'top_themes': [item[0] for item in  feature_aggregation['theme'].most_common(10)],
        'top_periods': [item[0] for item in  feature_aggregation['setting_period'].most_common(2)],
        'top_locations': [item[0] for item in  feature_aggregation['setting_location'].most_common(2)],
        'top_audiences': [item[0] for item in  feature_aggregation['target_audience'].most_common(2)],
        'read_authors': list(set(all_authors)),
        'read_isbns': read_isbns,

        'avg_input_google_rating': (total_google_rating_points / total_google_ratings_count_for_avg) if total_google_ratings_count_for_avg > 0 else None,
        'total_input_google_ratings_count': total_google_ratings_count_for_avg,

        'weighted_genres': dict(feature_aggregation['genre']),
        'weighted_tones': dict(feature_aggregation['tone']),
        'weighted_themes': dict(feature_aggregation['theme']),
        'weighted_setting_periods': dict(feature_aggregation['setting_period']),
        'weighted_setting_locations': dict(feature_aggregation['setting_location']),
        'weighted_target_audiences': dict(feature_aggregation['target_audience'])
    }

    print(f"Generated User Profile: {json.dumps(user_profile, indent=2)}")
    return user_profile

def background_book_analysis_task(book_list_data):
    redis_connection = Redis(decode_responses=True)
    db_path = 'data/books.db'

    analysed_books_dict = {}

    sqlite_conn = None
    try:
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_cursor = sqlite_conn.cursor()
        print(f"Connected to SQLite DB: {db_path} for updates.")
    except sqlite3.Error as e:
        print(f"ERROR: Could not connect to SQLite DB {db_path} in background_book_analysis_task: {e}")

    for book in book_list_data:
        isbn = book.get('isbn')
        if not isbn:
            continue

        llm_analysis = get_llm_analysis_for_book_local(book, redis_connection)

        current_book_result = book.copy()

        if llm_analysis:
            current_book_result['llm_genre'] = llm_analysis.get('genre')
            current_book_result['llm_setting_period'] = llm_analysis.get('setting_period')
            current_book_result['llm_setting_location'] = llm_analysis.get('setting_location')
            current_book_result['llm_tone'] = llm_analysis.get('tone')
            current_book_result['llm_target_audience'] = llm_analysis.get('target_audience')
            current_book_result['llm_themes'] = llm_analysis.get('themes')
            current_book_result['llm_sentiment'] = llm_analysis.get('sentiment')

            if sqlite_conn:
                try:
                    sqlite_cursor.execute("SELECT llm_themes FROM books WHERE isbn13 = ?", (isbn,))
                    existing_llm_data = sqlite_cursor.fetchone()

                    needs_update= True
                    if existing_llm_data and existing_llm_data[0] is not None:
                        pass
                    if needs_update:
                        print(f"Attempting to update books.db for ISBN: {isbn} with new LLM data.")
                        sqlite_cursor.execute("""
                                      UPDATE books
                                      SET llm_genre = ?,
                                          llm_themes = ?,
                                          llm_tone = ?,
                                          llm_setting_period = ?,
                                          llm_setting_location = ?,
                                          llm_target_audience = ?,
                                          llm_sentiment = ?,
                                          description = ?,
                                          google_categories = ?
                                      WHERE isbn13 = ?
                                      """, (
                                          json.dumps(book.get('llm_genre', [])),
                                          json.dumps(book.get('llm_themes', [])),
                                          json.dumps(book.get('llm_tone', [])),
                                          book.get('llm_setting_period'),
                                          book.get('llm_setting_location'),
                                          book.get('llm_target_audience'),
                                          book.get('llm_sentiment'),
                                          book.get('description'),
                                          json.dumps(book.get('categories', [])),
                                          isbn
                                      ))
                        if sqlite_cursor.rowcount == 0:
                            print(f"Warning: ISBN {isbn} not found in books.db for UPDATE.")
                        else:
                            sqlite_conn.commit()
                            print(f"Successfully updated books.db for ISBN: {isbn}")
                except sqlite3.Error as e:
                    print(f"ERROR: Failed to update books.db for ISBN: {isbn}: {e}")
                    if sqlite_conn: sqlite_conn.rollback()

        else:
            current_book_result['llm_genre'] = None
            current_book_result['llm_setting_period'] = None
            current_book_result['llm_setting_location'] = None
            current_book_result['llm_tone'] = None
            current_book_result['llm_target_audience'] = None
            current_book_result['llm_themes'] = None
            current_book_result['llm_sentiment'] = None
        
        analysed_books_dict[isbn] = current_book_result
            
    if sqlite_conn:
        sqlite_conn.close()
        print("Closed SQLite DB connection.")

    print("Generating user profile based on analysed books...")
    user_profile = generate_user_profile(analysed_books_dict)

    print("Finish background analysis and profile generation.")
    return {"analysed_books_map": analysed_books_dict, "user_profile_details": user_profile}


def calculate_similarity(user_profile, candidate_book_db_row):
    """
    Calculates a similarity score between the user profile and a candidate book from the database.
    candidate_book_db_row is a dict-like object (e.g., sqlite3.Row) from books.db.
    LLM fields like llm_genre, llm_themes, llm_tone are expected to be JSON strings from DB.
    """
    score = 0
    if not user_profile or not candidate_book_db_row:
        return 0

    # Helper to safely parse JSON strings (which might be list or single string) from DB fields into lowercase sets
    def safe_json_loads_to_lowercase_set(json_str):
        if json_str and isinstance(json_str, str):
            try:
                loaded = json.loads(json_str)
                if isinstance(loaded, list):
                    return set(item.lower() for item in loaded if isinstance(item, str) and item.strip())
                elif isinstance(loaded, str) and loaded.strip(): # Handle if LLM returned a single string instead of list
                    return {loaded.lower()}
            except json.JSONDecodeError:
                return set()
        return set()

    # Candidate's features (parsed from JSON strings stored in DB)
    candidate_genres = safe_json_loads_to_lowercase_set(candidate_book_db_row.get('llm_genre'))
    candidate_tones = safe_json_loads_to_lowercase_set(candidate_book_db_row.get('llm_tone'))
    candidate_themes = safe_json_loads_to_lowercase_set(candidate_book_db_row.get('llm_themes'))
    candidate_setting_period = (candidate_book_db_row.get('llm_setting_period') or "").lower()
    candidate_setting_location = (candidate_book_db_row.get('llm_setting_location') or "").lower()
    candidate_target_audience = (candidate_book_db_row.get('llm_target_audience') or "").lower()
    # Authors for the candidate book (already a JSON string list in DB from populate_db.py)
    try:
        candidate_authors = set(a.lower() for a in json.loads(candidate_book_db_row.get('authors') or "[]"))
    except json.JSONDecodeError:
        candidate_authors = set()


    # --- Scoring Logic (Weights can be tuned) ---
    WEIGHTS = {
        'genre': 7,
        'tone': 5,
        'theme': 2,             # Per shared theme
        'setting_period': 3,
        'setting_location': 3,
        'target_audience': 4,
        'author_match_boost': 5 # If user likes authors, a small boost if candidate matches
    }

    # User profile features (already lowercase lists/sets)
    user_top_genres_set = set(user_profile.get('top_genres', []))
    user_top_tones_set = set(user_profile.get('top_tones', []))
    user_top_themes_set = set(user_profile.get('top_themes', []))
    user_top_setting_periods_set = set(user_profile.get('top_setting_periods', []))
    user_top_setting_locations_set = set(user_profile.get('top_setting_locations', []))
    user_top_target_audiences_set = set(user_profile.get('top_target_audiences', []))
    user_read_authors_set = set(user_profile.get('read_authors', []))


    # Genre matching
    score += len(user_top_genres_set.intersection(candidate_genres)) * WEIGHTS['genre']

    # Tone matching
    score += len(user_top_tones_set.intersection(candidate_tones)) * WEIGHTS['tone']
    
    # Theme matching
    score += len(user_top_themes_set.intersection(candidate_themes)) * WEIGHTS['theme']

    # Setting Period matching
    if candidate_setting_period and candidate_setting_period in user_top_setting_periods_set:
        score += WEIGHTS['setting_period']
            
    # Setting Location matching
    if candidate_setting_location and candidate_setting_location in user_top_setting_locations_set:
        score += WEIGHTS['setting_location']

    # Target Audience matching
    if candidate_target_audience and candidate_target_audience in user_top_target_audiences_set:
        score += WEIGHTS['target_audience']
    
    # Author Boost (if candidate author is among user's read authors)
    if not user_read_authors_set.isdisjoint(candidate_authors):
        score += WEIGHTS['author_match_boost']

    return score


def generate_recommendations(analyzed_user_books, db_path='data/books.db', top_n=10):
    """
    Generates book recommendations based on the user's analyzed books.
    """
    user_profile = generate_user_profile(analyzed_user_books) # This is already up-to-date
    if not user_profile or not user_profile.get('read_isbns'):
        print("User profile is empty or invalid, cannot generate recommendations.")
        return []

    # print("User Profile for Recommendations:", json.dumps(user_profile, indent=2)) # Already printed in generate_user_profile

    scored_candidates = []
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row # Access columns by name, like a dictionary
        cursor = conn.cursor()

        # Prepare placeholders for excluding read ISBNs
        placeholders = ','.join('?' for _ in user_profile['read_isbns'])
        
        # Query to fetch candidate books that have been enriched by the LLM
        sql_query = f"""
            SELECT isbn13, title, authors, 
                   llm_genre, llm_themes, llm_tone, 
                   llm_setting_period, llm_setting_location, llm_target_audience
                   -- Include average_rating, ratings_count if they exist and are populated in your books.db
                   -- For now, assuming they are mostly NULL for candidates from the new CSV
                   --, average_rating, ratings_count 
            FROM books
            WHERE (llm_themes IS NOT NULL AND llm_themes != '[]') 
              AND (llm_genre IS NOT NULL AND llm_genre != '[]')
              AND isbn13 NOT IN ({placeholders})
        """
        
        query_params = user_profile['read_isbns']

        # OPTIONAL: Pre-filter candidates by user's top genre(s) using SQL for efficiency
        # eg if user_profile['top_genres'] has ['sci-fi', 'fantasy']
        if user_profile.get('top_genres'):
            genre_conditions = []
            # Create a temporary list for query_params because we might add to it
            current_query_params = list(query_params) # Start with read_isbns
            for genre_to_match in user_profile['top_genres']:
                genre_conditions.append(f"llm_genre LIKE ?")
                current_query_params.append(f'%"{genre_to_match}"%')
            
            if genre_conditions:
                sql_query += " AND (" + " OR ".join(genre_conditions) + ")"
                query_params = tuple(current_query_params)
        
        # Add a LIMIT to the SQL query
        sql_query += " LIMIT 1000"

        cursor.execute(sql_query, query_params)
        candidate_rows = cursor.fetchall()

        print(f"Fetched {len(candidate_rows)} candidate books from DB for scoring after genre filter (if any).")

        for candidate_row in candidate_rows:
            score = calculate_similarity(user_profile, candidate_row)
            
            if score > 0: # Only consider books with some similarity
                try:
                    authors_list = json.loads(candidate_row['authors'] or "[]")
                except json.JSONDecodeError:
                    authors_list = []
                
                recommended_book = {
                    'isbn': candidate_row['isbn13'],
                    'title': candidate_row['title'],
                    'authors': authors_list,
                    'score': score,
                }
                scored_candidates.append(recommended_book)
        
        scored_candidates.sort(key=lambda x: x['score'], reverse=True)

        print(f"Returning top {min(top_n, len(scored_candidates))} recommendations out of {len(scored_candidates)} scored candidates.")
        return scored_candidates[:top_n]

    except sqlite3.Error as e:
        print(f"Database error during recommendation generation: {e}")
        return []
    except Exception as e:
        import traceback
        print(f"Unexpected error during recommendation generation: {e}")
        traceback.print_exc()
        return []
    finally:
        if conn:
            conn.close()