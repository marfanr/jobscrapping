import asyncio
import logging
import random

from urllib.parse import quote_plus, urljoin
from pathlib import Path
import requests
from bs4 import BeautifulSoup

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from ..loader import registerJob
from ..jobscrapper import Jobscrapper
from ..utils import get_text

logger = logging.getLogger(__name__)

"""
For LinkedIn, I’m not scraping directly from LinkedIn; instead, I’m scraping through Google Search.
I`m use zenrows to bypass captha when google detected as a bot
"""
@registerJob("linkedin")
class LinkedinScrapper:

    def __init__(self, scrapper: Jobscrapper):
        self.config = scrapper.config
        self.scrapper = scrapper

        self.jobs_per_page = 25
        self.max_pages = 5

    async def execute(self):
        logger.info("Starting LinkedIn scraper")

        portal_config = self.config["portals"]

        if "linkedin" not in portal_config:
            return

        self.linkedin = portal_config["linkedin"]

        if self.linkedin.get("enable") is False:
            return

        target_markets = self.linkedin["target_market"]
        random.shuffle(target_markets)
        
        for target in target_markets:
            keyword = target["keyword"]
            locations = target["locations"]
            max_search_pages = int(target["max_search_pages"] or "0")
            

            random.shuffle(locations)

            for loc in locations:
                try:
                    logger.info(f"Scraping: {keyword} in {loc}")
                    for p in range(max_search_pages):
                        await self.scrap_job(keyword, loc, p)

                except PlaywrightTimeoutError as e:

                    logger.error(f"Timeout scraping " f"{keyword} in {loc}: {e}")
                    continue
                
                except KeyboardInterrupt:
                    return

                except Exception as e:

                    logger.exception(f"Error scraping " f"{keyword} in {loc}: {e}")
                    continue


    async def scrap_job(self, keyword, loc, page_number=0):
        print(f"fetch... {page_number}")
        start = page_number * 10 
        query = f'site:linkedin.com/jobs/view "{keyword}" "{loc}"'
        url = "https://www.google.com/search?" f"q={quote_plus(query)}" f"&num=10&tbs=qdr:w&start={start}"

        try:
            apikey = self.config["zenrows"]["key"]
        
        except:
            raise "zenrows key must be provided"
        
        params = {
            "url": url,
            "apikey": apikey,
            "mode": "auto",
        }
        response = requests.get("https://api.zenrows.com/v1/", params=params)
        page = await self.scrapper.context.new_page()
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            
            search = soup.select_one("#search")
            if not search:
                print("Search results container not found")
                return

            results = search.select("a[jsname='UWckNb']")

            print(f"Found {len(results)} results")

            for i, result in enumerate(results):
                href = result.get("href")
                href = urljoin("https://www.google.com", href)
                await page.goto(href, wait_until='domcontentloaded', timeout=60000)
                
                print(f"Final URL: {page.url}")

                if "linkedin.com/jobs/" not in page.url:
                    print(f"Not a LinkedIn job page: {page.url}")
                    continue
                
                await self.parse_jobs(page)

        else:
            print(f"Error: {response.status_code}")
            print(response.text)
        await page.close()
        
    async def parse_jobs(self, page: Page):
        topcard = page.locator(".topcard__flavor")
        company_name = await get_text(topcard.first)
        job_name = await get_text(page.locator(".topcard__title"))
        location = await get_text(topcard.nth(1))
        if company_name is None and job_name is None:
            return
        
        listed_date = await get_text(page.locator("[class^='posted-time-ago_']"))
        num_applicant = await get_text(page.locator(".num-applicants__caption"))
        recruiter = page.locator(".message-the-recruiter")
        
        show_more_less = page.locator(".show-more-less-html__markup")
        closed = page.locator("figcaption.closed-job__flavor--closed")
        
        
        print(f"company : {company_name}")
        print(f"job name : {job_name}")
        print(f"listed : {listed_date}")
        print(f"location : {location}")
        print(f"num applicant : {num_applicant}")
        
        if await recruiter.count() > 0:
            recruiter_name = await get_text(recruiter.locator("span.sr-only"))
            recruiter_link = await recruiter.locator("a[data-tracking-control-name='public_jobs']").get_attribute('href')
            print(f"recruiter : {recruiter_name}\n{recruiter_link}\n")

        show_more_less_count = await show_more_less.count()
        if show_more_less_count > 0:
            requirements = show_more_less.locator("ul").first.locator("li")
            print("requirements:\n")
            for r in await requirements.all():
                print(await r.inner_text())
                
            if show_more_less_count > 1:
                jobdescs = show_more_less.locator("ul").nth(1).locator("li")
                print("jobdescs:\n")
                for r in await jobdescs.all():
                    print(await r.inner_text())
        
        criteria = page.locator("ul.description__job-criteria-list li")

        job_criteria = {}

        for i in range(await criteria.count()):
            item = criteria.nth(i)

            key = await item.locator(
                "h3.description__job-criteria-subheader"
            ).inner_text()

            value = await item.locator(
                "span.description__job-criteria-text"
            ).inner_text()

            job_criteria[key.strip()] = value.strip()

        print(job_criteria)            
        if (await closed.count() > 0):
                    print("NO LONGER ACCEPT\n")
                    
        print()
        
        