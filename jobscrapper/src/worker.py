import json
import asyncio
import re
from kafka import KafkaConsumer, JsonSerializer
from .etl import ETL
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .silvers import Silvers
from .bronze import Bronze

class Worker:
    def __init__(self, config_path: str):
        with open(config_path, "r") as file:
            self.config = json.load(file)
            
        bs_server = self.config["kafka_bootstrap"] if self.config["kafka_bootstrap"] != None else "localhost:9092"
        self.consumer = KafkaConsumer(
            "rawjobs",
            bootstrap_servers=bs_server,
            value_deserializer=JsonSerializer(),
            group_id="rawjobs"
        )
        
        self.silvers = Silvers(self.config)
        self.bronze = Bronze(self.config)
                
        self.skill_list = None
        if self.config["skill_list"] is not None:
            self.skill_list = pd.read_csv(self.config["skill_list"])
            
        self.majors_list = None
        if self.config["majors_list"] is not None:
            self.majors_list = pd.read_csv(self.config["majors_list"])
       
        self.etl = ETL(self.skill_list, self.majors_list)
        
    
    def run(self):
        try:
            print("Worker run")
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("Worker stopped.")

    async def run_async(self):
        for record in self.consumer:
            if record.topic == "rawjobs":
                await self.listen_rawjobs(record.value)
    
    def _try_get(self, data: dict, key: str):
        if key in data.keys():
            return data[key]
        else:
            return None
    
    def parse_listing_date(self, listing_date: str | None):
        if not listing_date:
            return None

        listing_date = re.sub(r"\s+", " ", listing_date.strip().lower())

        timezone = self.config["timezone"] or "Asia/Jakarta"
        now = datetime.now(ZoneInfo(timezone))

        # hari ini
        if listing_date == "hari ini":
            return now

        # kemarin
        if listing_date == "kemarin":
            return now - timedelta(days=1)

        # X jam yang lalu
        match = re.search(
            r"(\d+)\+?\s*jam\s+yang\s+lalu",
            listing_date
        )

        if match:
            hours = int(match.group(1))
            return now - timedelta(hours=hours)

        # X hari yang lalu
        match = re.search(
            r"(\d+)\+?\s*hari\s+yang\s+lalu",
            listing_date
        )

        if match:
            days = int(match.group(1))
            return now - timedelta(days=days)

        print(f"Unknown listing date format: {listing_date}")
        return None
        
    async def listen_rawjobs(self, record):
        print(f"job name: {record['job_name']}")
        
        details = self._try_get(record, "details")
        salary = self._try_get(record, "salary")
        location = self._try_get(record, "location")
        highlights = self._try_get(record, "highlights")
        listing_date = self._try_get(record, "listing_date")
        source = self._try_get(record, "source")
        requirements = self._try_get(record, "requirements")
        skills = self._try_get(record, "skills")
        benefits = self._try_get(record, "benefits")
        data = self.etl.extract(details)
        company = self._try_get(record, "company")
        keyword = self._try_get(record, "keywoard")
        applicant = self._try_get(record, "applicant")
        quota = self._try_get(record, "quota")
        
        
        if company is None:
            return
        
        
        # save raw data
        self.bronze.save_raw(record, source, keyword)
        
        normalized_salary = []
        if salary is not None:
            salary_ = salary.strip()
            if source == "jobstreet":
                formated_salarys = re.findall(r"\d[\d.]*", salary_)
                for s in formated_salarys:
                    s_ = s.replace(".", "")
                    normalized_salary.append(int(s_))
            
            elif source == "glints":
                formated_salarys = salary_ \
                        .split("-")
                for s in formated_salarys:
                    mul = 1
                    if "jt" in salary_:
                        mul = 1000000
                    if "rb" in salary_:
                        mul = 1000
                        
                    s_ = s.replace("Rp ", "") \
                        .replace("jt", "") \
                        .replace("rb", "") \
                        .replace(",", ".")
                    s_ = float(s_) * mul
                    normalized_salary.append(int(s_))
        else:
            normalized_salary = None
            
        formated_listing_date = self.parse_listing_date(listing_date)
        
        skills_ = []
        if skills is not None:
            skills_.extend(l for l in skills)
        if data['skills'] is not None:
            skills_.extend(l for l in data['skills'])
            
        print(f"portal: {source}\n"
              f"keywoard: {keywoard}"
        )
        
        majors_ = data['majors'] 
        normalized_salary_ = [0, 0]
        if normalized_salary != None:
            normalized_salary_[0] = normalized_salary[0]
            if len(normalized_salary) > 1:
                normalized_salary_[1] = normalized_salary[1]
            
        job_name = record['job_name']
        url = record['url']

        if location is not None and source == "linkedin":
            location = self.etl.handle_location(location)
        
        self.silvers.insert(
            company=company,
            skills=skills_,
            majors=majors_,
            salary=normalized_salary_,
            name=job_name,
            url=url,
            details=details,
            source=source,
            requirements=requirements,
            location=location,
            listing_date=formated_listing_date,
            benefits=benefits,
            highlights=highlights,
            keyword=keyword
        )
        
        