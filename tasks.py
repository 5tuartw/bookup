import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from book_data import book_list
from fuzzywuzzy import fuzz

analyser = SentimentIntensityAnalyzer()

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

def process_book_list(user_book_titles):
    results = []

    book_titles_list = [book["title"].lower().strip() for book in book_list]

    for user_title in user_book_titles:
        user_title_processed = user_title.lower().strip()
        possible_matches = []

        for i, book_title in enumerate(book_titles_list):
            similarity_score = fuzz.ratio(user_title_processed, book_title)
            if similarity_score >= 60:
                possible_matches.append({
                    "match": book_list[i],
                    "similarity": similarity_score
                })

        if possible_matches:
            possible_matches.sort(key=lambda item: item["similarity"], reverse=True)
            results.append({"user_title": user_title, "possible_matches": possible_matches})
        else:
            results.append({"user_title": user_title, "possible_matches": []})
    
    return {"results_per_title": results}
    