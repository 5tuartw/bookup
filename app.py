from flask import Flask, request, render_template, jsonify
from rq import Queue
from redis import Redis
from tasks import process_book_list
from book_data import book_list
import time


app = Flask(__name__)
redis_conn = Redis()
q = Queue(connection=redis_conn)

@app.route('/', methods=['GET', 'POST'])
def index():
    book_list_text = ""
    if request.method == 'POST':
        book_list_text = request.form['book_list'] # uses the text area 'name' attribute
        user_book_titles = book_list_text.split("\n")
        job = q.enqueue(process_book_list, user_book_titles)
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

if __name__ == '__main__':
    app.run(debug=True)