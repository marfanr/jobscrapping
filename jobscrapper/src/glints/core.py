import asyncio
from playwright.async_api import async_playwright, Locator

import json
from ..loader import registerJob
from ..jobscrapper import Jobscrapper
from ..utils import get_text
import random

@registerJob("glints")
class GlintsScrapper:
    def __init__(self, scrapper: Jobscrapper):
        self.config = scrapper.config
        self.scrapper = scrapper
        self._processed_jobs = set()
        
    async def execute(self):
        self.scrapper.spawn(self.job())
    
    async def job(self):
        print("Glints Scrapper Job Started")
        portal_config = self.config["portals"]
        if 'glints' not in portal_config.keys():
            return

        jstreet = portal_config['glints']
        if 'enable' in jstreet and jstreet['enable'] == False:
            return
        
        target_markets = jstreet['target_market']
        random.shuffle(target_markets)
        
        for target in target_markets:
            keywoard = target["keywoard"]
            locations_ids = target["location_ids"]
            random.shuffle(locations_ids)
            for loc in locations_ids:
                await self.scrap_job(keywoard, loc)        
        
    
    async def scrap_job(self, keywoard, location_id):
        page = await self.scrapper.context.new_page()
        try:
            print(keywoard)
            url = f"https://glints.com/id/opportunities/jobs/explore?keyword={keywoard}&country=ID&locationId={location_id}&lowestLocationLevel=3"
            print(url)
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=60000
            )

            await page.wait_for_selector("#app", timeout=60000, state="attached")
            print(await page.title())
            
            await page.wait_for_selector("[role='article']", timeout=60000, state="attached")
            
            articles = page.locator("[role='article']")
            
            for article in await articles.all():
                await self.gather_job_data(article, self.scrapper.context)
        
        finally:    
            await page.close()
            
    async def gather_job_data(self, job: Locator, context):
        job_name = await get_text(job.locator("h2"))
        company_name = await get_text(job.locator("a[aria-label^='Job card company name:']"))
        if (job_name, company_name) in self._processed_jobs:
            return
        
        listed_time = await get_text(job.locator("p[data-recent='false']"))
        linkloc = job.locator("a[aria-label^='Job card title:']")
        link = "https://glints.com" + await linkloc.first.get_attribute('href')
        salary = await get_text(job.locator("span[class*='SalaryWrapper']"))
        location = await get_text(job.locator("span[class^='CardJobLocation__LocationWrapper']"))
        
        newpage = await context.new_page()
        try:
            await newpage.goto(
                link, wait_until="domcontentloaded"
            )
            
            requirements = set()
            jobrequirement_loc = newpage.locator("[class^='JobRequirementssc__TagsWrapper']")
            for i, c in enumerate(await jobrequirement_loc.all()):
                requirements.add((await c.inner_text()).strip("\n\r"))
            
            skills = set()
            skills_loc = newpage.locator("[class^='Skillssc__TagContainer-'] p")
            for i, c in enumerate(await skills_loc.all()):
                skills.add((await c.inner_text()).strip("\n\r"))
            
            benefits = set()
            benefit_loc = newpage.locator("[class^='Benefitssc__TagContainer'] p")
            for i, c in enumerate(await benefit_loc.all()):
                benefits.add((await c.inner_text()).strip("\n\r"))
            
            hrd_acc_name = await get_text(newpage.locator("[class^='ManagedBysc__CreatorName']"))
            lastonline = await get_text(newpage.locator("[class^='ManagedBysc__CreatorLastActive-']"))
            jobdesc = await get_text(newpage.locator("[aria-label='Job Description']"))
            
            gathered_job = {
                "job_name": job_name,
                "url": link,
                "salary": salary,
                "location": location,
                "listing_date": listed_time,
                "details": jobdesc,
                "source": "glints",
                "requirements": list(requirements),
                "skills": list(skills),
                "benefits": list(benefits),
                "publisher": {
                    "name": hrd_acc_name,
                    "last_online": lastonline
                },   
            }
    
            # publish kafka
            self.scrapper.producer.send('rawjobs', gathered_job)
            print("glints done")
            self._processed_jobs.add((job_name, company_name))
        
        finally:
            await newpage.close()