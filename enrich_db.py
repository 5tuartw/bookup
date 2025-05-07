from tasks import get_llm_analysis_for_book_local

import sqlite3
import requests
import json
import time
import redis
from redis import Redis, RedisError
import os

DB_FILE_PATH = 'data/books.db'
SLEEP_INTERVAL = 0.0 # Adjust as needed for Ollama setup

def enrich_database_llm_only():
    """Fetches only LLM analysis for books in the database."""
    print("Starting database enrichment (LLM only)...")
    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"ERROR: Could not connect to database {DB_FILE_PATH}: {e}")
        return

    # Persistent Redis connection for caching across the script run
    try:
        redis_conn = Redis(decode_responses=True)
        redis_conn.ping()
        print("Connected to Redis for caching.")
    except Exception as e:
        print(f"WARNING: Could not connect to Redis. Caching disabled. {e}")
        redis_conn = None

    # Check for NULL in llm_themes
    cursor.execute("SELECT isbn13, title, authors FROM books WHERE llm_themes IS NULL")
    books_to_process = cursor.fetchall()
    total_books = len(books_to_process)
    print(f"Found {total_books} books to process.")

    processed_count = 0
    updated_count = 0
    llm_errors = 0

    for book_row in books_to_process:
        processed_count += 1
        isbn = book_row['isbn13']
        title = book_row['title']
        authors = book_row['authors']

        print(f"\nProcessing {processed_count}/{total_books}: ISBN {isbn} - {title}")

        # Get LLM Analysis (Requires title, authors, isbn)
        llm_analysis_result = None
        if redis_conn:
            print(" - Getting LLM analysis...")
            book_data_for_llm = {
                'isbn': isbn,
                'title': title,
                'authors': authors
            }
            llm_analysis_result = get_llm_analysis_for_book_local(book_data_for_llm, redis_conn)

            if llm_analysis_result:
                print("   - LLM analysis successful.")
            else:
                print("   - LLM analysis failed or returned invalid data.")
                llm_errors += 1
        else:
            print(" - Skipping LLM analysis (Redis connection failed).")
            llm_errors += 1

        # Update Database if successful
        if llm_analysis_result:
            try:
                print(" - Updating database...")
                update_cursor = conn.cursor()
                update_cursor.execute("""
                    UPDATE books
                    SET llm_genre = ?,
                        llm_themes = ?,
                        llm_tone = ?,
                        llm_setting_period = ?,
                        llm_setting_location = ?,
                        llm_target_audience = ?,
                        llm_sentiment = ?
                        -- Note: We are NOT updating description or google_categories
                    WHERE isbn13 = ?
                """, (
                    json.dumps(llm_analysis_result.get('genre', [])),
                    json.dumps(llm_analysis_result.get('themes', [])),
                    json.dumps(llm_analysis_result.get('tone', [])),
                    llm_analysis_result.get('setting_period'),
                    llm_analysis_result.get('setting_location'),
                    llm_analysis_result.get('target_audience'),
                    llm_analysis_result.get('sentiment'),
                    isbn
                ))
                if processed_count % 50 == 0: # Commit every 50 records
                     conn.commit()
                     print(f"   - Committed batch at record {processed_count}")
                updated_count += 1
            except sqlite3.Error as e:
                print(f"   - Error updating database for {isbn}: {e}")
                conn.rollback() # Rollback failed update if needed

        # 3. Sleep
        time.sleep(SLEEP_INTERVAL)

    # Final commit for any remaining records
    try:
        conn.commit()
        print("Final commit successful.")
    except sqlite3.Error as e:
        print(f"Error during final commit: {e}")

    conn.close()
    print("\n--- Enrichment Summary ---")
    print(f"Total books needing processing: {total_books}")
    print(f"Attempted processing: {processed_count}")
    print(f"Rows updated in DB: {updated_count}")
    print(f"LLM analysis errors/skips: {llm_errors}")
    print("-------------------------")


# --- Main Execution ---
if __name__ == "__main__":
    enrich_database_llm_only()