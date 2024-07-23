import sqlite3
import json
import re
import pandas as pd
from PIL import Image, UnidentifiedImageError
import pytesseract
from io import BytesIO
import base64
import binascii
from tqdm import tqdm

# Load stock tickers from JSON
with open('final_project/utils/company_tickers.json', 'r') as f:
    stock_tickers = json.load(f)

tickers = {v['ticker']: v['title'] for k, v in stock_tickers.items()}

# Connect to the SQLite database
conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')

# Load the posts table into a DataFrame
posts_df = pd.read_sql_query('SELECT url, title, sub_reddit, author, post_date, upvotes, body, comments, image, scraped_date FROM posts', conn)

# Function to extract text from images using OCR
def extract_text_from_image(image_data):
    try:
        # Fix incorrect padding
        missing_padding = len(image_data) % 4
        if missing_padding != 0:
            image_data += '=' * (4 - missing_padding)
        
        # Decode base64 image data
        decoded_data = base64.b64decode(image_data)
        image = Image.open(BytesIO(decoded_data))
        
        # Convert image to a consistent format (e.g., PNG)
        with BytesIO() as converted_image:
            image = image.convert("RGB")  # Convert to RGB if not already
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

# Function to search for stock tickers in text
def find_tickers(text, tickers):
    found_tickers = []
    for ticker in tickers:
        if re.search(r'\b' + re.escape(ticker) + r'\b', text, re.IGNORECASE):
            found_tickers.append(ticker)
    return found_tickers

# Initialize columns for extracted data
posts_df['mentioned_tickers'] = ''
posts_df['image_text'] = ''

# Process each post with a progress bar
for index, row in tqdm(posts_df.iterrows(), total=posts_df.shape[0], desc="Processing posts"):
    combined_text = f"{row['title']} {row['body']} {row['comments']}"
    
    # Find tickers in text fields
    mentioned_tickers = find_tickers(combined_text, tickers)
    
    # Find tickers in image if it exists
    image_text = ''
    if row['image'] and row['image'].strip():
        image_text = extract_text_from_image(row['image'])
        mentioned_tickers.extend(find_tickers(image_text, tickers))
    
    # Remove duplicates and join tickers into a string
    mentioned_tickers = ', '.join(set(mentioned_tickers))
    
    # Update the DataFrame
    posts_df.at[index, 'mentioned_tickers'] = mentioned_tickers
    posts_df.at[index, 'image_text'] = image_text

# Filter out rows without any mentioned tickers
mentions_df = posts_df[posts_df['mentioned_tickers'] != '']

# Save the mentions DataFrame to a new table in the SQLite database
mentions_df.to_sql('stock_mentions', conn, if_exists='replace', index=False)

# Close the connection
conn.close()
