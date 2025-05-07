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
            f"1. genre: List the primary genre(s) \
                (e.g., 'science fiction, 'historical fantasy', 'thriller')\n"
            f"2. setting_period: State the primary time period \
                (e.g., 'contemporary', 'Victorian era', 'futuristic')\n"
            f"3. setting_location: Describe the primary location or type of setting \
                (e.g., 'London, England', 'Small town USA', 'space station', 'fictional kingdrom')\n"
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
                Do not include any text outside of the JSON object."
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

def background_book_analysis_task(book_list_data):
    redis_connection = Redis(decode_responses=True)
    db_path = 'data/books.db'

    results = {}

    sqlite_conn = None
    try:
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_cursor = sqlite_conn.cursor()
        print(f"Connect to SQLite DB: {db_path} for updates.")
    except sqlite3.Error as e:
        print(f"ERROR: Could not connect to SQLite DB {db_path} in background_book_analysis_task: {e}")

    for book in book_list_data:
        isbn = book.get('isbn')
        if not isbn:
            continue

        llm_analysis = get_llm_analysis_for_book_local(book, redis_connection)

        if llm_analysis:
            book['llm_genre'] = llm_analysis.get('genre')
            book['llm_setting_period'] = llm_analysis.get('setting_period')
            book['llm_setting_location'] = llm_analysis.get('setting_location')
            book['llm_tone'] = llm_analysis.get('tone')
            book['llm_target_audience'] = llm_analysis.get('target_audience')
            book['llm_themes'] = llm_analysis.get('themes')
            book['llm_sentiment'] = llm_analysis.get('sentiment')
        
        if sqlite_conn:
            try:
                print(f"Updating books.db for ISB {isbn} with new LLM data.")
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
                sqlite_conn.commit()
                print(f"Succesfully updated books.db for ISBN: {isbn}")
            except sqlite3.Error as e:
                print(f"ERROR: Failed to update books.db for ISBN: {isbn}: {e}")
                sqlite_conn.rollback()

        else:
            book['llm_genre'] = None
            book['llm_setting_period'] = None
            book['llm_setting_location'] = None
            book['llm_tone'] = None
            book['llm_target_audience'] = None
            book['llm_themes'] = None
            book['llm_sentiment'] = None
        
        results[isbn] = book
    
    if sqlite_conn:
        sqlite_conn.close()
        print("Closed SQLite DB connection.")

    print("Finish background analysis.")
    return results

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
    if not analysed_user_books:
        return {}
    
    # from the LLM analysis task
    all_genres = []
    all_tones = []
    all_themes = []
    all_setting_periods = []
    all_setting_locations = []
    all_target_audiences = []
    all_authors = []
    read_isbns = list(analysed_user_books.keys())

    # from the Google Books API
    total_google_rating_score = 0
    total_google_ratings_count_score = 0
    books_with_ratings = 0

    for isbn, book_details in analysed_user_books.item():
        if not book_details:
            continue

        # llm derived lists
        genres = book_details.get('llm_genre', [])
        tones = book_details.get('llm_tone', [])
        themes = book_details.get('llm_themes', [])
        all_genres.extend(g.lower() for g in genres if g)
        all_tones.extend(t.lower() for t in tones if t)
        all_themes.extend(th.lower() for th in themes if th)

        # llm derived strings
        period = book_details.get('llm_setting_period')
        if period: all_setting_periods.append(period.lower())
        location = book_details.get('llm_setting_location')
        if location: all_setting_locations.append(location.lower())
        audience = book_details.get('llm_target_audience')
        if audience: all_target_audiences.append(audience.lower())

        authors_list = book_details.get('authors', [])
        all_authors.extend(a.lower() for a in authors_list if a)

        avg_rating = book_details.get('averageRating')
        ratings_count = book_details.get('ratingsCount')
        if avg_rating is not None and ratings_count is not None:
            try:
                total_google_rating_score += float(avg_rating)
                total_google_ratings_count_score += int(ratings_count)
                books_with_ratings += 1
            except (ValueError, TypeError):
                pass
        
    genre_counts = Counter(all_genres)
    tone_counts = Counter(all_tones)
    theme_counts = Counter(all_themes)
    period_counts = Counter(all_setting_periods)
    location_counts = Counter(all_setting_locations)
    audience_counts = Counter(all_target_audiences)
    author_counts = Counter(all_authors)

    user_profile = {
        'top_genres': [item[0] for item in  genre_counts.most_common(3)],
        'top_tones': [item[0] for item in  tone_counts.most_common(3)],
        'top_themes': [item[0] for item in  theme_counts.most_common(3)],
        'top_periods': [item[0] for item in  period_counts.most_common(3)],
        'top_locations': [item[0] for item in  location_counts.most_common(3)],
        'top_audiences': [item[0] for item in  audience_counts.most_common(3)],
        'read_authors': list(author_counts.keys()),
        'avg_google_rating_preference': (total_google_rating_score / books_with_ratings) if books_with_ratings > 0 else None,
        'avg_google_popularity_preference': (total_google_ratings_count_score / books_with_ratings) if books_with_ratings > 0 else None,
        'genre_counts': dict(genre_counts),
        'tone_counts': dict(tone_counts),
        'theme_counts': dict(theme_counts),
        'audience_counts': dict(audience_counts)
    }

    print(f"Generated User Profile: {user_profile}")
    return user_profile
