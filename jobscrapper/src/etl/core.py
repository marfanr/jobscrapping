from .skill_extractor import SkillsExtractor
from .major_extractor import MajorExtractor

import pandas as pd
import re

class ETL:
    def __init__(self, skill_list, majors_list):
        self.skill_extractor = SkillsExtractor(skill_list)
        self.major_extractor = MajorExtractor(majors_list)
        self.provinces = pd.read_csv('./datasets/provinces.csv')
        self.cities = pd.read_csv('./datasets/cities.csv')
    
    def extract(self, raw_data: str):
        skills = self.skill_extractor.extract(raw_data)
        majors = self.major_extractor.extract(raw_data)
        return {
            "skills": skills,
            "majors": majors
        } 
    
    def normalize_location(self, name: str) -> str:
        name = name.lower().strip()
        name = re.sub(
            r"^(kabupaten|kab\.?|kota)\s+",
            "",
            name,
        )

        # Normalize whitespace
        name = re.sub(r"\s+", " ", name)

        return name.strip()
    
    def contains_location(self, location: str, target: str) -> bool:
        location = self.normalize_location(location)
        target = self.normalize_location(target)

        pattern = rf"\b{re.escape(target)}\b"

        return re.search(pattern, location) is not None
    
    def handle_location(self, loc: str):
        prov_list = self.provinces["name"].tolist()
        prov_list_en = self.provinces["name_en"].tolist()
        city_list = self.cities["name"].tolist()

        loc = self.normalize_location(loc)
        print(f"normalized loc: {loc}")

        prov_detected = None

        # Province
        for p in sorted(prov_list, key=len, reverse=True):
            if self.contains_location(loc, p):
                prov_detected = p
                break
            
        if prov_detected is None:
            for p in sorted(prov_list_en, key=len, reverse=True):
                if self.contains_location(loc, p):
                    prov_rows = self.provinces[
                        self.provinces["name_en"].str.lower() == p.lower()
                    ]
                    prov_detected = prov_rows["name"].iloc[0]
                    break

        # City
        if prov_detected is None:
            for city in sorted(city_list, key=len, reverse=True):
                if not self.contains_location(loc, city):
                    continue

                city_rows = self.cities[
                    self.cities["name"].str.lower() == city.lower()
                ]

                if city_rows.empty:
                    continue

                provid = city_rows["provid"].iloc[0]

                province = self.provinces[
                    self.provinces["id"] == provid
                ]

                if not province.empty:
                    prov_detected = province["name"].iloc[0]
                    break

        print(f"detected prov: {prov_detected}")

        if prov_detected is not None:
            return prov_detected.lower()
        
        return None