import pandas as pd
import sqlite3
import json
import math
from isbnlib import to_isbn13, is_isbn10, is_isbn13, clean as clean_isbn_string

# --- Configuration ---
CSV_FILE_PATH = 'data/book_data.csv'
DB_FILE_PATH = 'data/books.db'

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
        df = pd.read_csv(
            CSV_FILE_PATH,
            dtype={
                'ISBN': str,
                'Book-Title': str,
                'Book-Author': str,
                'Year-Of-Publication': str,
                'Publisher': str
            },
            on_bad_lines='warn',
            low_memory=False
        )
        print(f"Loaded {len(df)} rows.")
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at '{CSV_FILE_PATH}'. Please update the path.")
        return None
    except Exception as e:
        print(f"ERROR: Failed to load CSV: {e}")
        return None

    required_cols = ['ISBN', 'Book-Title', 'Book-Author']
    for col in required_cols:
        if col not in df.columns:
            print(f"ERROR: Required column '{col}' not found in CSV. Available columns: {list(df.columns)}")
            return None
    
    df_cleaned = df.copy()
    
    # --- Title Cleaning ---
    print("Cleaning 'Book-Title' column...")
    df_cleaned['Book-Title'] = df_cleaned['Book-Title'].astype(str).str.strip()
    # Remove leading/trailing double quotes only if they are at the very start/end
    df_cleaned['Book-Title'] = df_cleaned['Book-Title'].apply(
        lambda x: x[1:-1] if x.startswith('"') and x.endswith('"') else x
    )
    df_cleaned['Book-Title'] = df_cleaned['Book-Title'].str.title() # Convert to Title Case
    df_cleaned = df_cleaned[df_cleaned['Book-Title'].str.strip() != ''] # Remove if title became empty
    df_cleaned.dropna(subset=['Book-Title'], inplace=True)
    print(f"{len(df_cleaned)} rows after title cleaning.")

    # --- ISBN Processing using isbnlib ---
    print("Processing ISBNs...")
    processed_isbns = []
    for index, row in df_cleaned.iterrows():
        original_isbn_str = str(row.get('ISBN', '')).strip()
        # Use isbnlib's clean function - it's good at removing junk
        cleaned_original_isbn = clean_isbn_string(original_isbn_str)

        final_isbn13 = None
        final_isbn10 = None

        # --- DEBUG PRINT START ---
        # print(f"DEBUG: Row Index: {index}, Original ISBN Str: '{original_isbn_str}', Cleaned Original: '{cleaned_original_isbn}'")
        # --- DEBUG PRINT END ---

        if is_isbn13(cleaned_original_isbn):
            final_isbn13 = cleaned_original_isbn
            # print(f"DEBUG: Valid ISBN-13 found: {final_isbn13}")
        elif is_isbn10(cleaned_original_isbn):
            final_isbn10 = cleaned_original_isbn
            converted_isbn13 = to_isbn13(cleaned_original_isbn) # Returns '' on failure
            # print(f"DEBUG: Is ISBN-10 ('{final_isbn10}'). Attempting conversion to ISBN-13: '{converted_isbn13}'")
            if is_isbn13(converted_isbn13): # is_isbn13('') will be False
                final_isbn13 = converted_isbn13
            # else:
                # print(f"DEBUG: Conversion from ISBN-10 to ISBN-13 failed for '{final_isbn10}'. final_isbn13 remains None.")
        # else:
            # if cleaned_original_isbn: # Only print if there was something to evaluate
                # print(f"DEBUG: Not a valid ISBN-10 or ISBN-13 after cleaning: '{cleaned_original_isbn}'")


        processed_isbns.append({
            'original_index': index,
            'db_isbn13': final_isbn13 if final_isbn13 else pd.NA, # Use pd.NA for proper dropna handling
            'db_isbn10': final_isbn10 if final_isbn10 else pd.NA
        })

    isbn_df = pd.DataFrame(processed_isbns).set_index('original_index')
    df_cleaned = df_cleaned.join(isbn_df)

    # IMPORTANT: Keep only rows where we successfully got a valid ISBN-13 for the PK
    # pd.NA will be treated as missing by dropna.
    original_row_count_before_dropna = len(df_cleaned)
    df_cleaned.dropna(subset=['db_isbn13'], inplace=True)
    # Additionally, ensure db_isbn13 is not an empty string if any somehow passed dropna
    # (though pd.NA should have handled cases where final_isbn13 was None/empty from conversion)
    df_cleaned = df_cleaned[df_cleaned['db_isbn13'].astype(str).str.strip() != '']

    print(f"Rows before db_isbn13 NA/empty drop: {original_row_count_before_dropna}, Rows after: {len(df_cleaned)}")
    print(f"{len(df_cleaned)} rows remaining after ISBN processing and validation (must have valid non-empty ISBN-13).")

    # --- Author Parsing ---
    print("Processing 'Book-Author' column...")
    df_cleaned['authors_json'] = df_cleaned['Book-Author'].apply(
        lambda x: json.dumps([str(x).strip()]) if pd.notna(x) and str(x).strip() else json.dumps([])
    )
    
    # --- Other Columns from your CSV ---
    df_cleaned['Year-Of-Publication'] = df_cleaned['Year-Of-Publication'].astype(str).str.strip()
    df_cleaned['Publisher'] = df_cleaned['Publisher'].astype(str).str.strip()

    print("Data cleaning finished.")
    print("\n--- Sample of df_cleaned before returning (first 5 rows): ---")
    print(df_cleaned[['ISBN', 'Book-Title', 'db_isbn13', 'db_isbn10', 'authors_json']].head())
    print("----------------------------------------------------------\n")
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
        val_db_isbn13 = row.get('db_isbn13')
        val_db_isbn10 = row.get('db_isbn10')
        # ... get other values for the tuple ...

        # --- Add this DEBUG print block ---
        if index < 5: # Print for the first 5 rows
            print(f"DEBUG INSERTING: Index: {index}")
            print(f"  Raw db_isbn13 from DataFrame: '{val_db_isbn13}' (Type: {type(val_db_isbn13)})")
            print(f"  Raw db_isbn10 from DataFrame: '{val_db_isbn10}' (Type: {type(val_db_isbn10)})")
            print(f"  Title: {row.get('Book-Title')}")
        # --- End DEBUG print block ---
        data_tuple = (
            row.get('db_isbn13'),       # PRIMARY KEY
            row.get('db_isbn10'),       # May be None if original was ISBN-13
            row.get('Book-Title'),
            row.get('authors_json'),
            None, # language_code
            None, # num_pages
            str(row.get('Year-Of-Publication', '')),
            None, # average_rating
            None, # ratings_count
            None, # text_reviews_count
            str(row.get('Publisher', ''))
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