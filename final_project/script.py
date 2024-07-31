import sqlite3

conn = sqlite3.connect('final_project/scraped_data/reddit_posts.db')
cursor = conn.cursor()

# drop the stock_mentions table if it exists
cursor.execute('DROP TABLE IF EXISTS stock_mentions')
conn.commit()

# close
cursor.close()