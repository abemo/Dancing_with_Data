import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3
import json
import re
import utils.config as config

# given a date, return the number of posts per stock ticker mentioned with a sentiment score
def mentions_and_sentiment(date, mentions_df, posts_df):
    return

# return the overall sentiment of each subreddit
def subreddit_sentiment(date, mentions_df, posts_df):
    return

# return the number of positive, neutral, and negative sentiments of all posts for a given date
def overall_sentiment(date, mentions_df, posts_df):
    """
    go through the mentions_df, get the corresponding posts from the posts_df using the url and matching the date 
    param to the scraped_date then get the sentiment from the extracted_data and count the number of positive, 
    neutral, and negative sentiments
    """
    # get the posts for the given date
    posts = posts_df[posts_df['scraped_date'] == date]
    # merge the mentions_df and posts_df on the url column ONLY the date matches
    merged_df = mentions_df.merge(posts, on='url')
    # get the sentiment from the extracted_data column
    merged_df['sentiment'] = merged_df['extracted_data'].apply(lambda x: json.loads(x)['sentiment'])
    # count the number of positive, neutral, and negative sentiments
    sentiment_counts = merged_df['sentiment'].apply(pd.Series).stack().value_counts()
    return sentiment_counts

# create and display a dashboard of multiple plots using matplotlib
def display_plots(date, mentions_df, posts_df):
    
    stock_sentiments = mentions_and_sentiment(date, mentions_df, posts_df)
    sub_sentiments = subreddit_sentiment(date, mentions_df, posts_df)
    overal_sentiment = overall_sentiment(date, mentions_df, posts_df)
    
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))
    # plot the number of posts per stock ticker mentioned as a bar graph, with color-coded sentiment
    axs[0].bar(stock_sentiments['ticker'], stock_sentiments['count'], color=stock_sentiments['sentiment'])
    axs[0].set_title('Number of Posts per Stock Ticker Mentioned')
    axs[0].set_xlabel('Stock Ticker')
    axs[0].set_ylabel('Number of Posts')
    axs[0].legend(['Positive', 'Neutral', 'Negative'])
    
    # plot the overall sentiment of each subreddit as a pie chart
    axs[1].pie(sub_sentiments['sentiment'], labels=sub_sentiments['subreddit'], autopct='%1.1f%%')
    axs[1].set_title('Overall Sentiment of Each Subreddit')
    
    # plot the number of positive, neutral, and negative sentiments of all posts as a pie chart
    axs[2].pie(overal_sentiment['count'], labels=['Positive', 'Neutral', 'Negative'], autopct='%1.1f%%')
    axs[2].set_title('Overall Sentiment of All Posts')
    
    plt.tight_layout()
    plt.show(block=True)
    
    
def main():
    # connect to the SQLite database
    conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')

    # load the stock_mentions table into a DataFrame
    mentions_df = pd.read_sql_query('SELECT extracted_data FROM stock_mentions', conn)
    # load the posts table into a DataFrame
    posts_df = pd.read_sql_query('SELECT * FROM posts', conn)

    display_plots('2024-07-16', mentions_df, posts_df)
    
    # close the connection
    conn.close()

if __name__ == '__main__':
    main()