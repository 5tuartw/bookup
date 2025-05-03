from flask import Flask, request, render_template, jsonify
from rq import Queue
from redis import Redis
from tasks import analyse_review
import time

app = Flask(__name__)
redis_conn = Redis()
q = Queue(connection=redis_conn)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        review_text= request.form['review_text']
        job = q.enqueue(analyse_review, review_text)
        return render_template('index.html', job_id=job.id)
    return render_template('index.html')

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