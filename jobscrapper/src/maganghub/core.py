import asyncio
from playwright.async_api import async_playwright, Locator

from ..loader import registerJob
from ..jobscrapper import Jobscrapper
from ..utils import get_text
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import random
import requests


@registerJob("maganghub")
class MaganghubScrapper:
    def __init__(self, scrapper: Jobscrapper):
        self.config = scrapper.config
        self.scrapper = scrapper
        self.maganghub_cities = self.get_cities()
        self.maganghub_provinces = self.get_provinces()
        self.base_url = "https://maganghub.kemnaker.go.id"
        self.tz = self.config.get("timezone") or "Asia/Jakarta"

    def get_cities(self):
        url = "https://api.kemnaker.go.id/maganghub/onboarding/v2/cities"
        response = requests.get(
            url,
            params={"limit": 10000},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", [])

    def get_provinces(self):
            url = "https://api.kemnaker.go.id/maganghub/onboarding/v2/provinces"
            response = requests.get(
                url,
                params={"limit": 10000},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            return payload.get("data", [])

    async def execute(self):
        portal_config = self.config["portals"]
        if "maganghub" not in portal_config.keys():
            return

        self.maganghub = portal_config["maganghub"]
        if "enable" in self.maganghub and self.maganghub["enable"] == False:
            return

        target_markets = self.maganghub["target_market"]
        random.shuffle(target_markets)

        for target in target_markets:
            keyword = target["keyword"]
            locations = target["locations"]
            random.shuffle(locations)
            for loc in locations:
                try:
                    print(keyword, loc)
                    await self.scrap_job(keyword, loc)
                except KeyboardInterrupt:
                    print(f"stopped")
                except Exception as e:
                    print(f"error ... {e}\n")

    async def scrap_job(self, keyword, location, idx: int = 1):
        loc_obj = None
        for _, v in enumerate(self.maganghub_cities):
            if location in v["name"]:
                loc_obj = v
                
        prov_id = loc_obj["province_id"]
        prov_obj = None
        for _, v in enumerate(self.maganghub_provinces):
            if prov_id in v["id"]:
                prov_obj = v
        

        page = await self.scrapper.context.new_page()
        try:
            url = (
                self.base_url
                + "/magang-nasional/lowongan"
                # f"?city_id%5B0%5D%5Bid%5D={loc_obj["id"]}"
                # f"&city_id%5B0%5D%5Blabel%5D={loc_obj['name']}&page={idx}"
            )

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

            max_pages_el = page.locator("nav[role='navigation'] li")
            max_pages = int(
                await max_pages_el.nth(await max_pages_el.count() - 2).inner_text()
            )
            print(f"found {max_pages} pages")

            for idx in range(max_pages):
                url = (
                    self.base_url + "/magang-nasional/lowongan"
                    f"?page={idx}"
                    # f"?city_id%5B0%5D%5Bid%5D={loc_obj["id"]}"
                    # f"&city_id%5B0%5D%5Blabel%5D={loc_obj['name']}"
                )

                await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                cards = page.locator("a[href^='/magang-nasional/lowongan/']")
                for c in await cards.all():
                    job_name = await get_text(c.locator("h3"))
                    company = await get_text(c.locator("p.text-foreground"))
                    field = await get_text(c.locator("p.truncate"))
                    
                    spans = c.locator(".text-muted-foreground")
                    works_day = await get_text(spans.locator("span.flex").nth(2))
                    href = "https://maganghub.kemnaker.go.id" + "".join(await c.get_attribute("href"))
                    quota_el = c.locator("div.inline-flex.items-center")
                    quota = await get_text(quota_el.first)
                    applicant = await get_text(quota_el.nth(1))
                    if "Ramah Disabilitas" in quota:
                        quota = await get_text(quota_el.nth(1))
                        applicant = await get_text(quota_el.nth(2))
                    
                    await self.scrap_job_detail(
                        link=href,
                        job_name=job_name,
                        company_name=company,
                        field=field,
                        works_day=works_day,
                        quota=quota,
                        applicant=applicant,
                        location=prov_obj['name']
                    )

        finally:
            await page.close()

        await asyncio.sleep(10)

    async def scrap_job_detail(
        self, 
        link: str,
        job_name: str,
        company_name: str,
        field: str,
        works_day: str,
        quota: int,
        applicant: int,
        location: str
        ):
        applicant = applicant or 0
        applicant = applicant or 0
        works_day = works_day or 0
        
        page = await self.scrapper.context.new_page()
        try:
            await page.goto(link, wait_until="domcontentloaded", timeout=60000)
            
            details = ""
            requirements = set()
            
            containers_loc = page.locator("div.mh-container.py-8 section")
            for c in await containers_loc.all():
                h2 = await c.locator("h2").inner_text()
                if h2 is not None:
                    if "Deskripsi Lowongan" in h2:
                        content = await c.locator("p").first.inner_text()
                        details += content
                    
                    elif "Kualifikasi" in h2:
                        content = await c.locator("span.text-muted-foreground").all_inner_texts()
                        for c in content:
                            requirements.add(c)   
                        
                    elif "Skill yang Bakal Kamu Dapat" in h2:
                        items = c.locator("div.flex.items-start.gap-3.p-3")
                        for i in await items.all():
                            name = await i.locator("div.text-sm.font-medium").inner_text()
                            flex = i.locator("div.flex.flex-wrap")
                            tipe = await flex.locator("span.capitalize").inner_text()
                            timeline = await get_text(flex.locator("span").nth(1))
                            content = await get_text(i.locator("p.text-xs.text-muted-foreground"))

                            details += f"{name}\n{tipe}-{timeline}\n{content}\n\n"
                            
                    elif "Lokasi Magang" in h2:
                        loc =  c.locator("iframe[title='Google Map Embed']")
                        location = None
                        if await loc.count() > 0:
                            details += await loc.get_attribute("src")

            now = datetime.now(ZoneInfo(self.tz))
            gathered_job = {
                "job_name": job_name,
                "url": link,
                "location": location,
                "details": details,
                "source": "maganghub",
                "requirements": ", ".join(list(requirements)),
                "company": company_name,
                "keyword": str.lower(field),
                "applicant": applicant,
                "quota": quota,
                "scraped_at": now.isoformat(),
            }
            
            print(f"receive: {job_name} at {company_name} (maganghub)")

            # publish kafka
            self.scrapper.producer.send('rawjobs', gathered_job, link.encode("utf-8"))
               
        # except     
        finally:
            await page.close()
