import asyncio
from playwright.async_api import async_playwright, Locator

import json
from ..loader import registerJob
from ..jobscrapper import Jobscrapper
from ..utils import get_text
import random

@registerJob("jobstreet")
class JobStreetScrapper:
    def __init__(
        self,
        scrapper: Jobscrapper
    ):
        self.config = scrapper.config
        self.scrapper = scrapper
        
    async def execute(self):
        portal_config = self.config["portals"]
        if 'jobstreet' not in portal_config.keys():
            return

        jstreet = portal_config['jobstreet']
        if 'enable' in jstreet and jstreet['enable'] == False:
            return
        
        
        target_markets = jstreet['target_market']
        random.shuffle(target_markets)
           
        for target in target_markets:
            keywoard = target["keywoard"]
            locations = target["locations"]
            random.shuffle(locations)
            for loc in locations:
                try:
                    print(keywoard, loc)
                    await self.scrap_job(keywoard, loc)
                except Exception:
                    print("error ... continued")
        
        
    
    async def scrap_job(self, keywoard, location):
        page = await self.scrapper.context.new_page()
        try:
            job = keywoard.replace(' ', '-')
            url = f"https://id.jobstreet.com/id/{job}-jobs/in-{location}?page=1"
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=60000
            )
            print(url)
            
            await page.wait_for_selector("#app", timeout=60000, state="attached")
        
            # Total jobs
            job_count_el = page.locator(
                "[data-automation='totalJobsMessage'] span"
            )

            job_count_text = await get_text(job_count_el)

            if job_count_text:
                job_count = int(job_count_text.split(" ")[0])
            else:
                job_count = 0
            
            if job_count < 0:
                return

            # Job list
            posts = page.locator(
                "[data-automation='search-result-job-list']"
            )
            job_lists = posts.locator(":scope > *")
            count = await job_lists.count()
            print(f"found result job list {count}")

            for job in await job_lists.all():
                await self.gather_job_data(job, self.scrapper.context, keywoard)

        except asyncio.CancelledError:
            pass
        
        finally:
            await page.close()
            
    async def gather_job_data(self, job: Locator, context, keywoard):
        job_name = await get_text(
                    job.locator("h3")
                )
    
        company = await get_text(
            job.locator("[data-automation='jobCompany']")
        )

        location = await get_text(
            job.locator("[data-automation='jobCardLocation']")
        )

        salary = await get_text(
            job.locator("[data-automation='jobSalary']")
        )
                
        href = job.locator("a[data-automation='job-list-view-job-link']")
        job_url = None

        if await href.count() > 0:
            job_url = await href.get_attribute("href")

            if job_url and job_url.startswith("/"):
                job_url = f"https://id.jobstreet.com{job_url}"

        joblisting_date_loc = job.locator("[data-automation='jobListingDate'] span")
        joblisting_date = None
        
        if await joblisting_date_loc.count() > 0:
            joblisting_date = await joblisting_date_loc.first.inner_text()

        
        highlights = []
        ul = job.locator("ul")
        if (await ul.count() > 0):
            lis = ul.locator(":scope > *")
            for li in await lis.all():
                highlights.append(await li.inner_text())

        
        newpage = await context.new_page()
        try:
            await newpage.goto(job_url, wait_until="domcontentloaded")
            details = newpage.locator("[data-automation='jobAdDetails']")
            details_content = await details.inner_text()
            
            gathered_job = {
                "company": company,
                "job_name": job_name,
                "url": job_url,
                "salary": salary,
                "location": location,
                "highlights": highlights,
                "listing_date": joblisting_date,
                "details": details_content,
                "source": "jobstreet",
                "keywoard": str.lower(keywoard)
            }
            
            # publish kafka
            self.scrapper.producer.send('rawjobs', gathered_job)
        finally:
            await newpage.close()