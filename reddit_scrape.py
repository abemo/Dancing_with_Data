"""
Scrape the top n hot posts from a given subreddit and save them to Elasticsearch

# TODO: create list of investment related subreddits to scrape
# TODO: update crawl to scrape the top n hot posts from the subreddit
# TODO: update save_to_elasticsearch to save the post url, title, upvotes, downvotes, comments, picture, and body to Elasticsearch
"""

import mechanicalsoup as ms
from urllib.parse import urljoin
import redis
from elasticsearch import Elasticsearch
import os
import multiprocessing

class Crawler():
    def __init__(self, sub_reddit, number_of_posts):
        self.sub_reddit = sub_reddit
        self.visited = set()
        self.browser = ms.StatefulBrowser()
        self.number_of_posts = number_of_posts

        
    def crawl(self, verbose=False) -> None:
        # While there are still links to visit, crawl the pages until the end URL is found
        while self.redis_client.llen(self.queue_key) > 0:
            # Pop from the front of the list
            current_url = self.redis_client.lpop(self.queue_key).decode('utf-8')
            self.visited.add(current_url)
            self.number_of_pages_visited += 1
            self.browser.open(current_url)
            if verbose: print(f"{self.number_of_pages_visited}: {current_url}")
            
            # save the page to Elasticsearch
            page_html = self.browser.get_current_page().prettify()
            self.save_to_elasticsearch(current_url, page_html)
              
            if current_url == self.end_url:
                if verbose: print(f"{self.number_of_pages_visited}: Reached the end URL: {self.end_url}")
                return self.number_of_pages_visited  # Exit after finding the end URL
            for link in self.browser.links():
                href = link.get('href')
                if href and self.is_valid_link(href):
                    full_url = urljoin(current_url, href)
                    if full_url not in self.visited and self.redis_client.lrem(self.queue_key, 0, full_url) == 0:
                        self.redis_client.rpush(self.queue_key, full_url)
        if verbose: print("End URL not found")
    
    def is_valid_link(self, link) -> bool:
        # Ensure the link starts with the subreddit url and is not an add or promotional post
        start_url = self.sub_reddit
        match link:
            case str() if not link.startswith(start_url):
                return False
            case _:
                return True

    def save_to_elasticsearch(self, url, html) -> None:
        try:
            self.es.index(index='scrape', body={'url': url, 'html': html})
        except ConnectionError as e:
            print(f"ConnectionError while saving to Elasticsearch: {e}")


class Scraper():
    def __init__(self, number_of_posts = 100):
        self.sub_reddits = ["wallstreetbets", "investing", "stocks", "trading", 
                            "forex", "tradingreligion", "technicalraptor", "investor", 
                             "etoro", "asktrading", "BullTrader", "finance", "forextrading", 
                             "wealthify", "investoradvice",]
        self.number_of_posts = number_of_posts
        
        # Store the HTML of visited pages in Elasticsearch
        username = 'elastic'
        password = os.getenv('ELASTIC_PASSWORD') # Value you set in the environment variable
        client = Elasticsearch(
            "http://localhost:9200",
            basic_auth=(username, password))
        self.es = client

    def build_sub_reddit_url(self, sub_reddit):
        return f"https://www.reddit.com/r/{sub_reddit}/hot/"
    
    def scrape_sub_reddit(self, sub_reddit):
        sub_reddit_url = self.build_sub_reddit_url(sub_reddit)
        crawler = Crawler(sub_reddit = sub_reddit, number_of_posts = self.number_of_posts)
        crawler.crawl(verbose=False)
        
    def scrape_all(self):
        """
        Scrape all the sub reddits in parallel
        """
        processes = []
        for sub_reddit in self.sub_reddits:
            p = multiprocessing.Process(target=self.scrape_sub_reddit, args=(sub_reddit,))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        

scrapey = Scraper()
scrapey.scrape_sub_reddit("wallstreetbets")
