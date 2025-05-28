from flask import Flask, request, render_template, jsonify
from rq import Queue
from redis import Redis
from tasks import find_books_via_google_search, extract_keywords_from_text, background_book_analysis_task
import requests
import json

app = Flask(__name__)
redis_conn = Redis()
q = Queue(connection=redis_conn)

@app.route('/', methods=['GET', 'POST'])
def index():
    book_list_text = ""
    if request.method == 'POST':
        book_list_text = request.form['book_list'] # uses the text area 'name' attribute
        user_book_titles = book_list_text.split("\n")
        job = q.enqueue(find_books_via_google_search, user_book_titles)
        return render_template('index.html', job_id=job.id, book_list_text=book_list_text)
    return render_template('index.html', book_list_text=book_list_text)

@app.route('/results/<job_id>')
def get_results(job_id):
    job = q.fetch_job(job_id)

    if job.is_finished:
        return jsonify(status="finished", result=job.result)
    elif job.is_failed:
        return jsonify(status="failed", error=str(job.exc_info))
    else:
        return jsonify(status="pending")
    
@app.route('/fetch_book_data', methods=['POST'])
def fetch_book_data():
    data = request.get_json()
    isbn_list = data.get("isbnList",[])
    book_data = {}

    for isbn in isbn_list:
        api_url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&langRestrict=en"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            api_data = response.json()
            if 'items' in api_data and api_data['items']:
                book_info = api_data['items'][0]['volumeInfo']
                description = book_info.get('description')
                keywords = []
                if description:
                    keywords = extract_keywords_from_text(description)

                book_data[isbn] = {
                    'isbn': isbn,
                    'title': book_info.get('title'),
                    'authors': book_info.get('authors', []),
                    'description': description,
                    'categories': book_info.get('categories', []),
                    'imageLinks': book_info.get('imageLinks', {}).get('thumbnail'),
                    'averageRating': book_info.get('averageRating'),
                    'ratingsCount': book_info.get('ratingsCount'),
                    'pageCount': book_info.get('pageCount'),
                    'keywords': keywords
                }
            else:
                book_data[isbn] = {'not_found': True}
        except requests.exceptions.RequestException as e:
            book_data[isbn] = {'error': str(e)}

    return jsonify(book_data)

@app.route('/enqueue_llm_analysis', methods=['POST'])
def enqueue_llm_analysis():
    detailed_book_data_dict = request.get_json()
    if not detailed_book_data_dict:
        return jsonify(error="No data received"), 400
    
    book_list_for_llm = [
        book for isbn, book in detailed_book_data_dict.items()
        if book and not book.get('not_found') and not book.get('error') and book.get('isbn')
    ]

    if not book_list_for_llm:
        return jsonify(error="No valid books found in the provided data to analyse."), 400
    
    try:
        job_llm = q.enqueue(background_book_analysis_task, book_list_for_llm)

        print(f"Enqueued LLM analysis job: {job_llm.id}")

        return jsonify(job_id=job_llm.id)
    except Exception as e:
        print(f"Error enqueuing LLM analysis task: {e}")
        return jsonify(error=f"Server error: failed to start analysis task."), 500

if __name__ == '__main__':
    app.run(debug=True)