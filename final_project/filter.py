import sqlite3
import json
import re
import pandas as pd
from openai import OpenAI
import os
import utils.config as config
import tqdm
import re

# Load your OpenAI API key from an environment variable or directly set it here
client = OpenAI(api_key=config.OPENAI_API_KEY)

# Load stock tickers from JSON
with open('final_project/utils/company_tickers.json', 'r') as f:
    stock_tickers = json.load(f)

tickers = {v['ticker']: v['title'] for k, v in stock_tickers.items()}
ticker_patterns = {ticker: re.compile(r'\b' + re.escape(ticker) + r'\b', re.IGNORECASE) for ticker in tickers}

# Function to call the OpenAI API to extract stock tickers and sentiment
def extract_tickers_and_sentiment(text):
    prompt = f"Here is a block of text scraped from Reddit. Return a list of python dictionaries format of any stock or company referenced, as well as a rating of how positive the post is about that stock or company. Only output the python list, nothing else.\n\n{text}"

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a sentiment analysis assistant, skilled in extracting stock tickers and sentiment from text."},
            {"role": "user", "content": prompt}],
        max_tokens=150,
        temperature=0.5,
    )
    content = completion.choices[0].message.content
    
    # use regex, to only select the [ ] and the content inside
    match = re.search(r'\[.*?\]', content, re.DOTALL)
    if match:
        return match.group()
    else:
        print("No match found")


def process_post(row):
    combined_text = f"{row['title']} {row['body']}"
    
    # Preprocess to check for stock tickers before making an API call
    if not any(pattern.search(combined_text) for pattern in ticker_patterns.values()):
        return (row['url'], json.dumps([]))  # Return empty list if no tickers are found
    
    # Call the OpenAI API to extract stock tickers and sentiment
    extracted_data = extract_tickers_and_sentiment(combined_text)
    
    return (row['url'], extracted_data)

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')
    
    # Create the stock_mentions table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stock_mentions (
            url TEXT PRIMARY KEY,
            extracted_data TEXT
        )
    ''')
    conn.commit()
    
    # Load the posts table into a DataFrame
    posts_df = pd.read_sql_query('SELECT url, title, body, comments, image FROM posts', conn)
    
    # Load the stock_mentions table into a DataFrame
    analyzed_df = pd.read_sql_query('SELECT url FROM stock_mentions', conn)
    analyzed_urls = set(analyzed_df['url'])
    
    # Filter out already analyzed posts
    unanalyzed_posts_df = posts_df[~posts_df['url'].isin(analyzed_urls)]
    
    # Limit to 100 posts per run
    unanalyzed_posts_df = unanalyzed_posts_df.head(250)
    
    if unanalyzed_posts_df.empty:
        print("No new posts to analyze.")
        return
    
    # Process each post sequentially
    results = []
    for row in tqdm.tqdm(unanalyzed_posts_df.to_dict('records')):
        result = process_post(row)
        results.append(result)
    
    # Create DataFrame from results
    mentions_df = pd.DataFrame(results, columns=['url', 'extracted_data'])
    
    # Save the mentions DataFrame to the stock_mentions table in the SQLite database
    mentions_df.to_sql('stock_mentions', conn, if_exists='append', index=False)
    
    # Close the connection
    conn.close()

if __name__ == '__main__':
    main()
