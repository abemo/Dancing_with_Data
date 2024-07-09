import mechanicalsoup as ms
from urllib.parse import urljoin
import redis
from elasticsearch import Elasticsearch
import os

class Crawler():
    def __init__(self, start_url, end_url):
        self.visited = set()
        # Use Redis to store the URLs to visit
        self.redis_client = redis.Redis()
        self.queue_key = 'to_visit'
        self.redis_client.rpush(self.queue_key, start_url)
        self.end_url = end_url
        self.browser = ms.StatefulBrowser()
        # Store the HTML of visited pages in Elasticsearch
        username = 'elastic'
        password = os.getenv('ELASTIC_PASSWORD') # Value you set in the environment variable
        client = Elasticsearch(
            "http://localhost:9200",
            basic_auth=(username, password))
        print(client.info())
        self.es = client

        
    def crawl(self, verbose=False) -> None:
        # While there are still links to visit, crawl the pages until the end URL is found
        while self.redis_client.llen(self.queue_key) > 0:
            # Pop from the front of the list
            current_url = self.redis_client.lpop(self.queue_key).decode('utf-8')
            self.visited.add(current_url)
            self.browser.open(current_url)
            if verbose: print(f"{current_url}")
            
            # save the page to Elasticsearch
            page_html = self.browser.get_current_page().prettify()
            self.save_to_elasticsearch(current_url, page_html)
              
            if current_url == self.end_url:
                if verbose: print(f"Reached the end URL: {self.end_url}")
                return  # Exit after finding the end URL
            for link in self.browser.links():
                href = link.get('href')
                if href and self.is_valid_link(href):
                    full_url = urljoin(current_url, href)
                    if full_url not in self.visited and self.redis_client.lrem(self.queue_key, 0, full_url) == 0:
                        self.redis_client.rpush(self.queue_key, full_url)
        if verbose: print("End URL not found")
    
    def is_valid_link(self, link) -> bool:
        # Ensure the link starts with "/wiki/" and is not a Wikipedia special or file page
        match link:
            case str() if not link.startswith("/wiki/"):
                return False
            case str() if link.startswith("/wiki/Special:"):
                return False 
            case str() if link.startswith("/wiki/File:"):
                return False
            case str() if link.startswith("/wiki/User:"):
                return False
            case str() if link.startswith("/wiki/Talk:"):
                return False
            case str() if link.startswith("/wiki/Category:"):
                return False
            case _:
                return True

    def save_to_elasticsearch(self, url, html) -> None:
        try:
            self.es.index(index='scrape', body={'url': url, 'html': html})
        except ConnectionError as e:
            print(f"ConnectionError while saving to Elasticsearch: {e}")

start_url = "https://en.wikipedia.org/wiki/Redis"
end_url = "https://en.wikipedia.org/wiki/Jesus"
crawler = Crawler(start_url, end_url)
crawler.crawl(verbose=True)
