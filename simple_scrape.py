import mechanicalsoup as ms
from urllib.parse import urljoin

class Crawler():
    def __init__(self, start_url, end_url):
        self.visited = set()
        self.to_visit = list()
        self.to_visit.append(start_url)
        self.end_url = end_url
        self.browser = ms.StatefulBrowser()
        
    def crawl(self):
        # While there are still links to visit, crawl the pages until the end URL is found
        while self.to_visit:
            # pop from the front of the list
            current_url = self.to_visit.pop(0)
            self.visited.add(current_url)
            self.browser.open(current_url)
            print(f"{current_url}")  
            if current_url == self.end_url:
                print(f"Reached the end URL: {self.end_url}")
                return  # Exit after finding the end URL
            for link in self.browser.links():
                href = link.get('href')
                if href and self.is_valid_link(href):
                    full_url = urljoin(current_url, href)
                    if full_url not in self.visited and full_url not in self.to_visit:
                        self.to_visit.append(full_url)
        print("End URL not found")
    
    def is_valid_link(self, link) -> bool:
        # Ensure the link starts with "/wiki/" and is not a Wikipedia special or file page
        match link:
            case str() if not link.startswith("/wiki/"):
                return False
            case str() if link.startswith("/wiki/Special:"):
                return False 
            case str() if link.startswith("/wiki/File:"):
                return False
            case _:
                return True


start_url = "https://en.wikipedia.org/wiki/Redis"
end_url = "https://en.wikipedia.org/wiki/Jesus"
crawler = Crawler(start_url, end_url)
crawler.crawl()
