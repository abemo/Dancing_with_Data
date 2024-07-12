import praw
from elasticsearch import Elasticsearch
import multiprocessing
import config

class Crawler():
    def __init__(self, sub_reddit, number_of_posts, es_client):
        self.sub_reddit = sub_reddit
        self.number_of_posts = number_of_posts
        self.es = es_client
        self.reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=config.REDDIT_USER_AGENT
        )

    def crawl(self, verbose=False) -> None:
        subreddit = self.reddit.subreddit(self.sub_reddit)
        hot_posts = subreddit.hot(limit=self.number_of_posts)

        for post in hot_posts:
            if not post.stickied:  # Skip the stickied post
                if verbose: print(f"Processing post: {post.title}")
                post_data = {
                    'url': post.url,
                    'title': post.title,
                    'author': str(post.author),
                    'post_date': post.created_utc,
                    'upvotes': post.score,
                    'body': post.selftext,
                    'comments': post.num_comments,
                    'image': post.url if post.url.endswith(('.jpg', '.png', '.gif')) else ''
                }
                # Save the post to Elasticsearch
                self.save_to_elasticsearch(post_data)
                if verbose: print(f"Saved post: {post.title}")

    def save_to_elasticsearch(self, post_data) -> None:
        try:
            self.es.index(index='scrape', body=post_data)
        except ConnectionError as e:
            print(f"ConnectionError while saving to Elasticsearch: {e}")


class Scraper():
    def __init__(self, number_of_posts=100, verbose=False):
        self.sub_reddits = ["wallstreetbets", "investing", "stocks", "trading", 
                            "forex", "tradingreligion", "technicalraptor", "investor", 
                             "etoro", "asktrading", "BullTrader", "finance", "forextrading", 
                             "wealthify", "investoradvice",]
        self.number_of_posts = number_of_posts
        self.verbose = verbose
        
        # Store the HTML of visited pages in Elasticsearch
        username = 'elastic'
        password = config.ELASTIC_PASSWORD
        client = Elasticsearch(
            "http://localhost:9200",
            basic_auth=(username, password))
        self.es = client
    
    def scrape_sub_reddit(self, sub_reddit):
        crawler = Crawler(sub_reddit=sub_reddit, number_of_posts=self.number_of_posts, es_client=self.es)
        crawler.crawl(self.verbose)
        
    def scrape_all(self):
        """
        Scrape all the subreddits in parallel
        """
        processes = []
        for sub_reddit in self.sub_reddits:
            p = multiprocessing.Process(target=self.scrape_sub_reddit, args=(sub_reddit, self.verbose))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()

scrapey = Scraper(verbose=True)
scrapey.scrape_sub_reddit("wallstreetbets")
# scrapey.scrape_all()
# search the elasticsearch index for all the titles of the posts
# search = Elasticsearch.search(index="scrape", body={"query": {"match_all": {}}})
# print the titles of the posts
# print(search['hits']['hits'])