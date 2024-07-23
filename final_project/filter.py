import sqlite3
import json
import re
import pandas as pd
from PIL import Image, UnidentifiedImageError
import pytesseract
from io import BytesIO
import base64
import binascii
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load stock tickers from JSON
with open('final_project/utils/company_tickers.json', 'r') as f:
    stock_tickers = json.load(f)

tickers = {v['ticker']: v['title'] for k, v in stock_tickers.items()}
ticker_patterns = {ticker: re.compile(r'\b' + re.escape(ticker) + r'\b', re.IGNORECASE) for ticker in tickers}

# Define contextual regex patterns
contextual_patterns = {
    ticker: re.compile(
        rf'\b(?:stock|shares|equities|buy|sell|trade|investment|price|value|market|traded)\s*[\s.,]*{re.escape(ticker)}\b',
        re.IGNORECASE
    )
    for ticker in tickers
}

# Function to extract text from images using OCR
def extract_text_from_image(image_data, target_size=(800, 800)):
    if not image_data:
        print("No image data provided.")
        return ""
    
    try:
        # Fix incorrect padding
        missing_padding = len(image_data) % 4
        if missing_padding != 0:
            image_data += '=' * (4 - missing_padding)
        
        # Decode base64 image data
        decoded_data = base64.b64decode(image_data)
        
        # Try to open the image
        try:
            image = Image.open(BytesIO(decoded_data))
        except Exception as e:
            print(f"Image opening error: {e}")
            return ""
        
        # Convert image to a consistent format (e.g., PNG) and resize
        with BytesIO() as converted_image:
            image = image.convert("RGB")  # Convert to RGB if not already
            image = image.resize(target_size, Image.ANTIALIAS)  # Resize image
            image.save(converted_image, format="PNG")
            converted_image.seek(0)
            image_data = converted_image.getvalue()
        
        # Extract text from the converted image
        return pytesseract.image_to_string(BytesIO(image_data))
    
    except binascii.Error as e:
        print(f"Base64 decoding error: {e}")
    except UnidentifiedImageError as e:
        print(f"Unidentified image error: {e}")
    except Exception as e:
        print(f"Unexpected error processing image: {e}")
    
    return ""

# Function to find tickers with context
def find_tickers(text, ticker_patterns, contextual_patterns):
    found_tickers = set()
    for ticker, pattern in ticker_patterns.items():
        if pattern.search(text):
            # Check if ticker is used in a contextual pattern
            context_pattern = contextual_patterns.get(ticker)
            if context_pattern and context_pattern.search(text):
                found_tickers.add(ticker)
    return list(found_tickers)

def process_post(row):
    combined_text = f"{row['title']} {row['body']} {row['comments']}"
    
    # Find tickers in text fields
    mentioned_tickers = find_tickers(combined_text, ticker_patterns, contextual_patterns)
    
    # Find tickers in image if it exists
    image_text = ''
    if row['image'] and row['image'].strip():
        image_text = extract_text_from_image(row['image'])
        mentioned_tickers.extend(find_tickers(image_text, ticker_patterns, contextual_patterns))
    
    # Remove duplicates and join tickers into a string
    mentioned_tickers = ', '.join(set(mentioned_tickers))
    
    return (row['url'], mentioned_tickers, image_text)

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')
    
    # Load the posts table into a DataFrame
    posts_df = pd.read_sql_query('SELECT url, title, body, comments, image FROM posts', conn)
    
    # Initialize multiprocessing
    pool = Pool(cpu_count())
    
    # Process each post in parallel with a progress bar
    results = []
    for result in tqdm(pool.imap_unordered(process_post, posts_df.to_dict('records')), total=posts_df.shape[0], desc="Processing posts"):
        results.append(result)
    
    # Close the pool
    pool.close()
    pool.join()
    
    # Create DataFrame from results
    mentions_df = pd.DataFrame(results, columns=['url', 'mentioned_tickers', 'image_text'])
    
    # Filter out rows without any mentioned tickers
    mentions_df = mentions_df[mentions_df['mentioned_tickers'] != '']
    
    # Save the mentions DataFrame to a new table in the SQLite database
    mentions_df.to_sql('stock_mentions', conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()

if __name__ == '__main__':
    main()
