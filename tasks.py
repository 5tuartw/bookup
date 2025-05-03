import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyser = SentimentIntensityAnalyzer()

def  analyse_review(review_text):
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
