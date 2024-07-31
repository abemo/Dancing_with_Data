import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3
import json

def filter_by_date(date, mentions_df, posts_df):
    mentions_on_date = mentions_df[mentions_df['scraped_date'] == date]
    posts_on_date = posts_df[posts_df['scraped_date'] == date]
    return mentions_on_date, posts_on_date

def mentions_and_sentiment(date, mentions_df, posts_df):
    mentions_on_date, _ = filter_by_date(date, mentions_df, posts_df)
    mentions_and_sentiment_counts = {}
    for _, mention in mentions_on_date.iterrows():
        if pd.notna(mention['extracted_data']) and len(mention['extracted_data'].strip()) > 0:
            try:
                contents = json.loads(mention['extracted_data'])
                if isinstance(contents, list):
                    for content in contents:
                        if isinstance(content, dict):
                            ticker = content.get('ticker', '').upper()
                            sentiment = content.get('sentiment', '').lower()
                            if ticker not in mentions_and_sentiment_counts:
                                mentions_and_sentiment_counts[ticker] = {'count': 0, 'sentiment': {'positive': 0, 'neutral': 0, 'negative': 0}}
                            mentions_and_sentiment_counts[ticker]['count'] += 1
                            if sentiment in mentions_and_sentiment_counts[ticker]['sentiment']:
                                mentions_and_sentiment_counts[ticker]['sentiment'][sentiment] += 1
                            else:
                                print(f"Unexpected sentiment label: {sentiment}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON for entry: {mention['extracted_data']}")
    return mentions_and_sentiment_counts

def subreddit_sentiment(date, mentions_df, posts_df):
    mentions_on_date, posts_on_date = filter_by_date(date, mentions_df, posts_df)
    subreddit_sentiment_counts = {}
    for _, post in posts_on_date.iterrows():
        subreddit = post['sub_reddit']
        if subreddit not in subreddit_sentiment_counts:
            subreddit_sentiment_counts[subreddit] = {'positive': 0, 'neutral': 0, 'negative': 0}
    for _, mention in mentions_on_date.iterrows():
        if pd.notna(mention['extracted_data']) and len(mention['extracted_data'].strip()) > 0:
            try:
                contents = json.loads(mention['extracted_data'])
                if isinstance(contents, list):
                    for content in contents:
                        if isinstance(content, dict):
                            sentiment = content.get('sentiment', '').lower()
                            url = mention.get('url')
                            scraped_date = mention.get('scraped_date')
                            if url and scraped_date:
                                post_row = posts_df[(posts_df['url'] == url) & (posts_df['scraped_date'] == scraped_date)]
                                if not post_row.empty:
                                    subreddit = post_row['sub_reddit'].values[0]
                                    if subreddit and sentiment in subreddit_sentiment_counts[subreddit]:
                                        subreddit_sentiment_counts[subreddit][sentiment] += 1
                                    else:
                                        print(f"Unexpected sentiment label: {sentiment}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON for entry: {mention['extracted_data']}")
    return subreddit_sentiment_counts

def overall_sentiment(date, mentions_df, posts_df):
    mentions_on_date, _ = filter_by_date(date, mentions_df, posts_df)
    positive_count = 0
    neutral_count = 0
    negative_count = 0
    for _, mention in mentions_on_date.iterrows():
        if pd.notna(mention['extracted_data']) and len(mention['extracted_data'].strip()) > 0:
            try:
                contents = json.loads(mention['extracted_data'])
                if isinstance(contents, list):
                    for content in contents:
                        if isinstance(content, dict):
                            sentiment = content.get('sentiment', '').lower()
                            if sentiment == 'positive':
                                positive_count += 1
                            elif sentiment == 'neutral':
                                neutral_count += 1
                            elif sentiment == 'negative':
                                negative_count += 1
                            else:
                                print(f"Unexpected sentiment label: {sentiment}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON for entry: {mention['extracted_data']}")
    return {'positive': positive_count, 'neutral': neutral_count, 'negative': negative_count}

def display_plots(date, mentions_df, posts_df):
    stock_sentiments = mentions_and_sentiment(date, mentions_df, posts_df)
    sub_sentiments = subreddit_sentiment(date, mentions_df, posts_df)
    total_sentiment = overall_sentiment(date, mentions_df, posts_df)
    
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))
    
    # Prepare data for stock sentiments plot
    tickers = list(stock_sentiments.keys())
    counts = [stock_sentiments[ticker]['count'] for ticker in tickers]
    positive = [stock_sentiments[ticker]['sentiment']['positive'] for ticker in tickers]
    neutral = [stock_sentiments[ticker]['sentiment']['neutral'] for ticker in tickers]
    negative = [stock_sentiments[ticker]['sentiment']['negative'] for ticker in tickers]
    
    bar_width = 0.35
    index = np.arange(len(tickers))
    
    # Bar plot for stock sentiments
    p1 = axs[0].bar(index, positive, bar_width, label='Positive')
    p2 = axs[0].bar(index, neutral, bar_width, bottom=positive, label='Neutral')
    p3 = axs[0].bar(index, negative, bar_width, bottom=np.array(positive) + np.array(neutral), label='Negative')
    
    axs[0].set_title('Number of Posts per Stock Ticker Mentioned')
    axs[0].set_xlabel('Stock Ticker')
    axs[0].set_ylabel('Number of Posts')
    axs[0].set_xticks(index)
    axs[0].set_xticklabels(tickers)
    axs[0].legend()
    
    # Pie chart for subreddit sentiments
    sub_sentiments_counts = {k: sum(v.values()) for k, v in sub_sentiments.items()}
    sub_sentiments_labels = list(sub_sentiments_counts.keys())
    sub_sentiments_sizes = list(sub_sentiments_counts.values())
    axs[1].pie(sub_sentiments_sizes, labels=sub_sentiments_labels, autopct='%1.1f%%')
    axs[1].set_title('Overall Sentiment of Each Subreddit')
    
    # Pie chart for overall sentiment
    total_counts = list(total_sentiment.values())
    total_labels = ['Positive', 'Neutral', 'Negative']
    axs[2].pie(total_counts, labels=total_labels, autopct='%1.1f%%')
    axs[2].set_title('Overall Sentiment of All Posts')
    
    plt.tight_layout()
    plt.show(block=True)

def main():
    conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')
    mentions_df = pd.read_sql_query('SELECT * FROM stock_mentions', conn)
    posts_df = pd.read_sql_query('SELECT * FROM posts', conn)
    display_plots('2024-07-16', mentions_df, posts_df)
    conn.close()

if __name__ == '__main__':
    main()
