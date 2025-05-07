import pandas as pd
import sqlite3
import json
import math

# --- Configuration ---
CSV_FILE_PATH = 'data/books.csv'
DB_FILE_PATH = 'data/books.db'
KEEP_LANGUAGES = ['eng', 'en-US', 'en-GB', 'en']

# --- Database Setup ---
def setup_database():
    """Creates the SQLite database and the books table if they don't exist."""
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()

    # Drop table if it exists
    cursor.execute("DROP TABLE IF EXISTS books")

    # Create table schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            isbn13 TEXT PRIMARY KEY,
            isbn10 TEXT,
            title TEXT NOT NULL,
            authors TEXT,         -- Store as JSON list string
            language_code TEXT,
            num_pages INTEGER,
            publication_date TEXT,
            average_rating REAL,
            ratings_count INTEGER,
            text_reviews_count INTEGER,
            publisher TEXT,
            -- Placeholders for enrichment data
            description TEXT,
            google_categories TEXT, -- Store as JSON list string
            llm_genre TEXT,         -- Store as JSON list string
            llm_themes TEXT,        -- Store as JSON list string
            llm_tone TEXT,          -- Store as JSON list string
            llm_setting_period TEXT,
            llm_setting_location TEXT,
            llm_target_audience TEXT,
            llm_sentiment TEXT
        )
    """)
    conn.commit()
    conn.close()
    print(f"Database '{DB_FILE_PATH}' setup complete.")

def load_and_clean_data():
    """Loads data from CSV, cleans it, and prepares for DB insertion."""
    print(f"Loading data from '{CSV_FILE_PATH}'...")
    try:
        # Adjusting dtype={'isbn': str, 'isbn13': str} if pandas misinterprets them
        df = pd.read_csv(CSV_FILE_PATH, dtype={'isbn': str, 'isbn13': str}, on_bad_lines='warn')
        print(f"Loaded {len(df)} rows.")
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at '{CSV_FILE_PATH}'. Please update the path.")
        return None
    except Exception as e:
        print(f"ERROR: Failed to load CSV: {e}")
        return None

    # Rename columns slightly for consistency if desired (optional)
    df.rename(columns={'       num_pages': 'num_pages'}, inplace=True) # Example if extra spaces

    columns_to_keep = [
        'title', 'authors', 'average_rating', 'isbn', 'isbn13',
        'language_code', 'num_pages', 'ratings_count',
        'text_reviews_count', 'publication_date', 'publisher'
    ]
    # Check which columns actually exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df_cleaned = df[existing_columns].copy()

    # Filter by language
    df_cleaned = df_cleaned[df_cleaned['language_code'].isin(KEEP_LANGUAGES)]
    print(f"{len(df_cleaned)} rows remaining after language filtering.")

    # Handle missing ISBNs (use ISBN13 as primary key)
    df_cleaned.dropna(subset=['isbn13'], inplace=True)
    print(f"{len(df_cleaned)} rows remaining after dropping missing ISBN13.")

    # Handle missing titles
    df_cleaned.dropna(subset=['title'], inplace=True)
    print(f"{len(df_cleaned)} rows remaining after dropping missing titles.")

    # Handle potentially problematic numeric/date columns (fill NaN with None or default)
    numeric_cols = ['average_rating', 'num_pages', 'ratings_count', 'text_reviews_count']
    for col in numeric_cols:
         if col in df_cleaned.columns:
              # Convert to numeric, coercing errors to NaN, then fill NaN with None (NULL in DB)
              df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
              df_cleaned[col] = df_cleaned[col].apply(lambda x: None if math.isnan(x) else x)


    # Fill NaN/NaT in other text columns with None
    text_cols = ['isbn', 'authors', 'publication_date', 'publisher', 'language_code']
    for col in text_cols:
         if col in df_cleaned.columns:
              df_cleaned[col].fillna('', inplace=True)

    # Format authors as JSON string list (optional, handles multiple authors)
    if 'authors' in df_cleaned.columns:
         df_cleaned['authors_json'] = df_cleaned['authors'].apply(
             lambda x: json.dumps([a.strip() for a in str(x).split('/')]) if pd.notna(x) and x else json.dumps([])
         )
    else:
         df_cleaned['authors_json'] = json.dumps([])


    print("Data cleaning finished.")
    return df_cleaned

def insert_data_to_db(df):
    """Inserts cleaned data from DataFrame into the SQLite database."""
    if df is None or df.empty:
        print("No data to insert.")
        return

    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    print(f"Inserting {len(df)} rows into database...")

    inserted_count = 0
    skipped_count = 0

    for index, row in df.iterrows():
        data_tuple = (
            row.get('isbn13'),
            row.get('isbn', ''), # Use isbn10 if available, else empty string
            row.get('title'),
            row.get('authors_json', json.dumps([])), # Use the JSON formatted authors
            row.get('language_code'),
            row.get('num_pages'),
            row.get('publication_date', ''),
            row.get('average_rating'),
            row.get('ratings_count'),
            row.get('text_reviews_count'),
            row.get('publisher', '')
        )

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO books (
                    isbn13, isbn10, title, authors, language_code, num_pages,
                    publication_date, average_rating, ratings_count, text_reviews_count, publisher
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data_tuple)
            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1
        except sqlite3.Error as e:
            print(f"Error inserting row {index} (ISBN13: {row.get('isbn13')}): {e}")
            skipped_count += 1

        # Commit periodically for large datasets
        if (index + 1) % 1000 == 0:
            conn.commit()
            print(f"Processed {index + 1} rows...")

    conn.commit()
    conn.close()
    print(f"Database insertion complete. Inserted: {inserted_count}, Skipped (duplicates/errors): {skipped_count}")

if __name__ == "__main__":
    setup_database()
    cleaned_df = load_and_clean_data()
    insert_data_to_db(cleaned_df)