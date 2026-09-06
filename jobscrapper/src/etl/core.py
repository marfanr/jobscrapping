import re
from .skill_extractor import SkillsExtractor
from .major_extractor import MajorExtractor

class ETL:
    def __init__(self, skill_list, majors_list):
        self.skill_extractor = SkillsExtractor(skill_list)
        self.major_extractor = MajorExtractor(majors_list)
    
    def extract(self, raw_data: str):
        skills = self.skill_extractor.extract(raw_data)
        majors = self.major_extractor.extract(raw_data)
        return {
            "skills": skills,
            "majors": majors
        }