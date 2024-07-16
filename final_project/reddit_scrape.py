import praw
import utils.config as config
import time
import sqlite3

class Crawler():
    def __init__(self, sub_reddit, number_of_posts, db_connection):
        self.sub_reddit = sub_reddit
        self.number_of_posts = number_of_posts
        self.db_connection = db_connection
        self.reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=config.REDDIT_USER_AGENT
        )

    def crawl(self, verbose=False) -> None:
        subreddit = self.reddit.subreddit(self.sub_reddit)
        hot_posts = subreddit.hot(limit=self.number_of_posts)

        for post in hot_posts:
            if not post.stickied:
                post_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(post.created_utc))
                today_date = time.strftime("%Y-%m-%d", time.localtime(time.time()))

                # Save the post data
                self.save_to_sqlite(
                    post.url,
                    post.title,
                    str(post.author),
                    post.created_utc,
                    post.score,
                    post.selftext,
                    post.num_comments,
                    post.url if post.url.endswith(('.jpg', '.png', '.gif')) else ''
                )

                if verbose:
                    print(f"Saved post: {post.title}")


    def save_to_sqlite(self, url, title, author, post_date, upvotes, body, comments, image) -> None:
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO posts (url, title, author, post_date, upvotes, body, comments, image, scraped_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url,
                title,
                author,
                post_date,
                upvotes,
                body,
                comments,
                image,
                time.strftime("%Y-%m-%d", time.localtime(time.time()))  # Store the current timestamp as the scraped_date
            ))
            self.db_connection.commit()
        except sqlite3.Error as e:
            print(f"Error saving to SQLite: {e}")
        finally:
            cursor.close()


class Scraper():
    def __init__(self, number_of_posts=100, verbose=False):
        self.sub_reddits = ["wallstreetbets", "investing", "stocks", "trading",
                            "forex", "algotrading", "investor", "etoro",
                            "asktrading", "finance", "forextrading", "investoradvice"]
        self.number_of_posts = number_of_posts
        self.verbose = verbose

        self.db_connection = sqlite3.connect('/Users/abe/git/Dancing_with_Data/final_project/scraped_data/reddit_posts.db')
        self.create_table()

    def create_table(self):
        cursor = self.db_connection.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    url TEXT,
                    title TEXT,
                    author TEXT,
                    post_date REAL,
                    upvotes INTEGER,
                    body TEXT,
                    comments INTEGER,
                    image TEXT,
                    scraped_date REAL,
                    primary key (url, scraped_date)
                )
            """)
            self.db_connection.commit()
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")
        finally:
            cursor.close()

    def scrape_sub_reddit(self, sub_reddit):
        crawler = Crawler(sub_reddit=sub_reddit, number_of_posts=self.number_of_posts, db_connection=self.db_connection)
        crawler.crawl(self.verbose)

    def scrape_all(self):
        if self.verbose:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        for sub_reddit in self.sub_reddits:
            if self.verbose:
                print("="*20)
                print(f"Scraping subreddit: {sub_reddit}")
                print("="*20)
            self.scrape_sub_reddit(sub_reddit)

    def __del__(self):
        if self.db_connection:
            self.db_connection.close()


scrapey = Scraper(verbose=True)
scrapey.scrape_all()
