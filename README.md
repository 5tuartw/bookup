# WIP 📔 bookup: Your Personal AI-Powered Book Recommendation Finder

## 📝 Description

Bookup is a web app designed to provide personalised book recommendations based on the user inputting a list of books they've read and enjoyed. The application then:
1.  Identifies the correct book editions using the Google Books API.
2.  Allows user confirmation of these matches.
3.  Fetches detailed metadata (like descriptions and categories) for the confirmed books.
4.  Performs in-depth content analysis using spaCy for keyword extraction and a locally run Large Language Model (LLM via Ollama, specifically Llama 3.1 8B as configured) to determine nuanced features like genre, themes, tone, setting (period/location), target audience, and overall sentiment.
5.  Generates a user preference profile based on this analysis, potentially weighting features from more popular/highly-rated input books.
6.  (Future Implementation) Compares this profile against an enriched database of candidate books to suggest new titles the user might like.

All heavy processing (API calls, LLM analysis) is handled by background tasks using Python RQ with Redis, ensuring a responsive user interface. LLM responses are cached in Redis to improve speed and reduce redundant processing. The application also includes offline scripts to populate and enrich a local SQLite database from a user-provided CSV data source.

## 🌟 Features

* **User Book List Input:** Simple textarea for users to list books.
* **Book Identification:** Uses Google Books API search to find potential matches for user-input titles.
* **User Confirmation UI:** Allows users to select the correct book from search results or exclude titles via a dynamic HTML table and radio buttons.
* **Detailed Data Fetching:** Retrieves metadata (description, categories, ratings, etc.) for confirmed books via Google Books API.
* **Keyword Extraction:** Uses spaCy to extract significant keywords from book descriptions.
* **Local LLM Analysis:** Uses Ollama (Llama 3.1 8B model) for deep content analysis to extract:
    * Genre(s)
    * Themes (prompted for single words or short phrases)
    * Tone(s)
    * Setting (Period & Location)
    * Target Audience
    * Overall Sentiment
* **Background Task Processing:** Utilises Python RQ and Redis for asynchronous processing of Google Books API calls and LLM analysis.
* **Caching:** LLM analysis results are cached in Redis to speed up subsequent requests for the same book.
* **User Preference Profile Generation:** Creates a profile based on aggregated and weighted features from the user's analysed books.
* **Profile Display:** Shows the user their analysed books, common themes derived from their list, and a summary of their deduced preferences.
* **Offline Data Management Scripts:**
    * `populate_db.py`: Loads an initial list of books from a user-provided CSV (with columns like ISBN, Book-Title, Book-Author, etc.) into an SQLite database (`books.db`). Includes title cleaning and ISBN-10 to ISBN-13 conversion using `isbnlib`.
    * `enrich_db.py`: Processes books in `books.db`, performing LLM analysis for each and storing the results back into the database. Designed to be resumable and uses Redis caching.

## 🖥️ Tech Stack

* **Backend:**
    * 🐍 Python 3.11
    * 🫙 Flask (Web framework)
    * 📃 RQ (Redis Queue - for background tasks)
    * 📨 Redis (Message broker for RQ and caching)
* **Frontend:**
    * HTML5
    * CSS3
    * Vanilla JavaScript (DOM manipulation, `fetch` API)
* **NLP & LLM:**
    * 🌌 spaCy (for keyword extraction)
    * 🦙 Ollama (for running local Large Language Models, e.g., Llama 3.1 8B)
    * 📗 `requests` library (for HTTP calls to Google Books API and Ollama)
* **Data Handling & Storage:**
    * 📅 SQLite (for `books.db` persistent book database)
    * 🐼 Pandas (for CSV processing in `populate_db.py`)
    * 📘 `isbnlib` (for ISBN validation and conversion in `populate_db.py`)

## 🔨 Setup and Installation

1.  **Prerequisites:**
    * Python 3.9+
    * Redis server installed and running.
    * Ollama installed. Pull a model, for example:
        ```bash
        ollama pull llama3.1:8b 
        ```
        *(Ensure the model name specified in `tasks.py` matches what you have pulled, e.g., `model_name = "llama3.1:8b"`)*
    * `pip` (Python package installer).

2.  **Clone the Repository (if applicable):**
    ```bash
    git clone <your_repository_url>
    cd bookup 
    ```
    (Or ensure you have all project files in a local directory)

3.  **Create and Activate a Python Virtual Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/macOS
    # OR
    # venv\Scripts\activate    # On Windows Command Prompt
    # .\venv\Scripts\Activate.ps1 # On Windows PowerShell
    ```

4.  **Install Python Dependencies:**
    Create a `requirements.txt` file in your project root with the following content:
    ```txt
    flask
    rq
    redis
    requests
    spacy
    pandas
    isbnlib
    # vaderSentiment # (If analyse_review in tasks.py is to be used)
    ```
    Then install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```
    Download the spaCy English model:
    ```bash
    python -m spacy download en_core_web_sm
    ```

5.  **Data Setup:**
    * Create a `data/` subdirectory in your project root if it doesn't exist.
    * Place your public domain book data CSV file (e.g., `my_books.csv` with headers like `ISBN,Book-Title,Book-Author,Year-Of-Publication,Publisher`) into this `data/` directory.
    * * I used this open source list, but you may want to use a smaller set: https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/data
    * Open `populate_db.py` and update the `CSV_FILE_PATH` variable at the top to point to your CSV file. For example:
        ```python
        CSV_FILE_PATH = 'data/my_books.csv' 
        ```
    * Run the script to create and populate `data/books.db` with basic book info:
        ```bash
        python populate_db.py
        ```

6.  **Data Enrichment (Long Process):**
    * Ensure your Ollama service is running (e.g., run `ollama list` in a new terminal to confirm).
    * Ensure your Redis server is running (check with `redis-cli ping`).
    * Run the enrichment script. This will process all books in `books.db` that haven't been analysed yet. It will take a very long time for a large dataset and is resumable.
        ```bash
        python enrich_db.py
        ```

## 🏃‍♀️‍➡️ Running the Application

1.  **Start Redis Server:** Ensure it's running.
2.  **Start Ollama Service:** Ensure it's running.
3.  **Start RQ Worker:**
    * Open a new terminal.
    * Navigate to the project directory.
    * Activate the virtual environment (`source venv/bin/activate` or `venv\Scripts\activate`).
    * Run: `rq worker`
    * Keep this terminal open.
4.  **Start Flask Application:**
    * Open another new terminal.
    * Navigate to the project directory.
    * Activate the virtual environment.
    * Run: `python app.py`
    * Keep this terminal open. You should see output like `* Running on http://127.0.0.1:5000/`.
5.  **Access in Browser:** Open your web browser and go to `http://127.0.0.1:5000/`.

## 👉 How to Use

1.  On the main page, enter a list of book titles you've read (one title per line) into the textarea.
2.  Click "Look up book titles."
3.  Wait for the system to find matches using the Google Books API.
4.  A table will appear showing your input titles alongside potential matches. For each of your input titles:
    * Select the radio button corresponding to the correct book.
    * Or, if none of the suggestions are correct or you want to exclude that title, select "None of these / Exclude this title."
5.  Click "Confirm Selections and Get Details."
6.  The system will fetch more details for your confirmed books and then start a background LLM analysis. The status message on the page will update.
7.  Once "Analysis complete!" appears, you will see:
    * A list of your "Analysed Books" with their LLM-derived sentiment.
    * A "Common Themes (from LLM Analysis)" section if any themes were identical across multiple books.
    * A "Your Deduced Preferences" section summarising your profile (top genres, tones, themes, etc.).
8.  *(The next step to implement is displaying actual book recommendations based on this profile).*

## 🏗️ Project Structure Overview
```bookup/
├── app.py              # Main Flask web application, routes
├── tasks.py            # RQ worker tasks (Google Search, spaCy, LLM analysis, profile, recommendations)
├── populate_db.py      # Script to populate SQLite DB from input CSV
├── enrich_db.py        # Script to enrich SQLite DB with LLM analysis for all books
├── data/
│   ├── your_books.csv  # Placeholder for the user's input CSV (update in populate_db.py)
│   └── books.db        # SQLite database (created and managed by scripts)
├── static/
│   ├── app.js          # Frontend JavaScript for UI interactivity
│   └── styles.css      # CSS for styling
├── templates/
│   └── index.html      # Main HTML page template
├── venv/               # Python virtual environment (standard practice)
└── README.md           # This file (you are reading it!)
```

## 📂 Key Files Summary

* **`app.py`**: Handles web requests from the user, enqueues background jobs to RQ, and serves the `index.html` template.
* **`tasks.py`**: Contains the functions executed by the RQ worker in the background. This includes searching Google Books for titles, extracting keywords with spaCy, calling the local LLM for detailed book analysis, generating user preference profiles, and (soon) generating recommendations.
* **`populate_db.py`**: A utility script to read a CSV file of books, clean the data (including ISBN conversion and title formatting), and insert it into the `books.db` SQLite database.
* **`enrich_db.py`**: An offline script that iterates through the books in `books.db`, calls the local LLM (via `get_llm_analysis_for_book_local` from `tasks.py`) to get detailed analysis (genre, themes, tone, etc.), and updates the database with this information. It uses Redis for caching LLM responses.
* **`static/app.js`**: Contains all the client-side JavaScript logic. It handles form submissions, polling for job statuses, dynamically updating the HTML to display results, user preferences, and (soon) recommendations.
* **`templates/index.html`**: The main HTML structure for the web application.
* **`data/books.db`**: The SQLite database storing the enriched catalogue of books used for generating recommendations.

## 🧙 Future Enhancements (Ideas)

* Implement and display the actual book recommendations based on the generated user profile and the enriched `books.db`.
* More sophisticated user profile visualisation (e.g., interactive word clouds for themes, genres, tones using a library like D3.js or WordCloud2.js).
* User accounts to save book lists, preference profiles, and rated recommendations.
* Ability for users to rate recommended books to refine future suggestions (collaborative filtering aspects).
* Option to enrich `books.db` with more metadata for candidate books, such as cover images, full descriptions, or ratings from APIs (if a reliable source for the large dataset is found or if specific books are looked up on demand).
* More advanced scoring and ranking algorithms for recommendations, potentially exploring different weighting schemes or machine learning models if a larger interaction dataset becomes available.
* Error handling and user feedback improvements throughout the application.
* Pagination for displaying large lists of results or recommendations.
