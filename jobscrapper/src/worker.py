import json
import asyncio
import re
from kafka import KafkaConsumer, JsonSerializer
from .etl import ETL
import pandas as pd

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
                
        self.skill_list = None
        if self.config["skill_list"] is not None:
            self.skill_list = pd.read_csv(self.config["skill_list"])
            
        self.majors_list = None
        if self.config["majors_list"] is not None:
            self.majors_list = pd.read_csv(self.config["majors_list"])
       
        self.etl = ETL(self.skill_list, self.majors_list)
        
    
    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("Scraper stopped.")

    async def run_async(self):
        for record in self.consumer:
            if record.topic == "rawjobs":
                await self.listen_rawjobs(record.value)
    
    def _try_get(self, data: dict, key: str):
        if key in data.keys():
            return data[key]
        else:
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
        publisher = self._try_get(record, "publisher")
        publisher_name = self._try_get(publisher, "name") if publisher is not None else None
        publisher_last_online = self._try_get(publisher, "last_online") if publisher is not None else None
        
        data = self.etl.extract(details)
        
        normalized_salary = []
        if salary is not None:
            salary = salary.strip()
            formated_salarys = re.findall(r"\d[\d.]*", salary)
            for s in formated_salarys:
                s_ = s.replace(".", "")
                normalized_salary.append(int(s_))
        else:
            normalized_salary = None
            
        print(f"extracted data: {data}\n"
              f"salary: {normalized_salary}\n",
              f"location: {location}\n",
              f"highlights: {highlights}\n",
              f"listing_date: {listing_date}\n"
              f"data: {data}\n"
              f"source: {source}\n"
              )
        