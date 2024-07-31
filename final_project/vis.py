import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3
import json

# given a date, return the number of posts per stock ticker mentioned with a sentiment score
def mentions_and_sentiment(date, mentions_df, posts_df):
    return

# return the overall sentiment of each subreddit
def subreddit_sentiment(date, mentions_df, posts_df):
    return

# return the number of positive, neutral, and negative sentiments of all posts for a given date
def overall_sentiment(date, mentions_df):
    """
    Go through the mentions_df, get the posts matching the date 
    then get the sentiment from the extracted_data and 
    count the number of positive, neutral, and negative sentiments
    """
    # Select only the mentions on the given date
    mentions_on_date = mentions_df[mentions_df['scraped_date'] == date]
    
    positive_count = 0
    neutral_count = 0
    negative_count = 0
    
    for _, mention in mentions_on_date.iterrows():
        # Check if extracted_data is not empty
        if pd.notna(mention['extracted_data']) and len(mention['extracted_data'].strip()) > 0:
            try:
                contents = json.loads(mention['extracted_data'])
                
                # Check if contents is a list of dictionaries
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
            except json.JSONDecodeError:
                print(f"Error decoding JSON for entry: {mention['extracted_data']}")
    
    return {'positive': positive_count, 'neutral': neutral_count, 'negative': negative_count}

        

# create and display a dashboard of multiple plots using matplotlib
def display_plots(date, mentions_df, posts_df):
    
    stock_sentiments = mentions_and_sentiment(date, mentions_df, posts_df)
    sub_sentiments = subreddit_sentiment(date, mentions_df, posts_df)
    overall_sentiment = overall_sentiment(date, mentions_df, posts_df)
    
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
    axs[2].pie(overall_sentiment['count'], labels=['Positive', 'Neutral', 'Negative'], autopct='%1.1f%%')
    axs[2].set_title('Overall Sentiment of All Posts')
    
    plt.tight_layout()
    plt.show(block=True)
    
    
def main():
    # connect to the SQLite database
    conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')

    # load the stock_mentions table into a DataFrame
    mentions_df = pd.read_sql_query('SELECT * FROM stock_mentions', conn)
    # load the posts table into a DataFrame
    posts_df = pd.read_sql_query('SELECT * FROM posts', conn)

    # display_plots('2024-07-16', mentions_df, posts_df)
    print(overall_sentiment('2024-07-16', mentions_df))
    
    # close the connection
    conn.close()

if __name__ == '__main__':
    main()