!pip install beautifulsoup4

import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

class Scraper:
    """
    Author: Sandeep Pawar   | Fabric.Guru   | Jun 19, 2023

    This class is responsible for scraping ideas from the Fabric ideas site

    Warning: Be careful with the value of max_pages. If you set it to 'max', it will
    scrape data from pages until no data is available. This could result in a large
    number of requests to the server, which may not be allowed by the site's policy.
    Always ensure that your scraping activities respect the site's terms of service.
    Use it only for learning purposes and avoid any PII data.

    """
    def __init__(self, url, experience_name, max_pages=None):
        self.base_url = url + "?page="
        self.dfs = []  # List to store dataframes
#         self.max_pages = max_pages
        if max_pages is None:
            self.max_pages = 3
        elif max_pages == -1:
            self.max_pages = 'max'
        else:
            self.max_pages = max_pages

        self.experience_name = experience_name

    def extract_data(self, idea):
        # Helper function to reduce redundancy
        def get_text(selector):
            element = idea.select_one(selector)
            return element.text.strip() if element else None

        def get_href(selector):
            element = idea.select_one(selector)
            return element.get('href') if element else None

        href = get_href(".mspb-mb-18 > .ms-card-heading-1 > A:nth-child(1):nth-last-child(1)")
        user_id = get_href(".idea-avatar")
        user_id = user_id.split('=')[-1] if user_id else None
        idea_id = href.split('/')[-1] if href else None

        return {
            "product_name": get_text("h2.mspb-mb-4.ms-card-heading-2"),
            "idea_id": idea_id.split("=")[1] if idea_id else None,
            "idea": get_text(".mspb-mb-18 A"),
            "votes": get_text(".ms-fontWeight-semibold *"),
            "date": get_text(".mspb-my-0 SPAN *"),
            "state": get_text(".ms-tag\-\-lightgreen"),
            "user_id": user_id,
            "like_count": get_text(".ms-like-count"),
            "description": get_text(".ms-fontColor-gray150.mspb-mb-12"),
        }

    def get_page_data(self, session, page):
        time.sleep(1)
        current_url = self.base_url + str(page)
        response = session.get(current_url)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.content, "html.parser")
        ideas = soup.select(".mspb-py-sm-24")
        if not ideas:  # If there are no matches
            return None
        return [self.extract_data(idea) for idea in ideas]

    def scrape_data(self):
        # Create a session
        with requests.Session() as session:
            page = 1
            while True:
                result = self.get_page_data(session, page)

                # If the page doesn't contain data, break the loop
                if not result:
                    break

                df_page = pd.DataFrame(result)
                self.dfs.append(df_page)

                # If a specific maximum number of pages is set and it has been reached, break the loop
                if self.max_pages != 'max' and page >= self.max_pages:
                    break

                page += 1

        # Concatenate all the dataframes into one
        df = pd.concat(self.dfs, ignore_index=True)
        df["Experience"] = self.experience_name
        return df

url = "https://ideas.fabric.microsoft.com/"
scraper = Scraper(url,"Power BI")
df_ideas = scraper.scrape_data()
