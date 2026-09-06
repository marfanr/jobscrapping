from pandas import DataFrame
from spacy.lang.id import Indonesian
from spacy.matcher import PhraseMatcher

class SkillsExtractor:
    def __init__(self, skills_list : DataFrame):
        self.skills_list = skills_list['skills'].to_list()
        self.indonesia_nlp = Indonesian()
        self.matcher = PhraseMatcher(self.indonesia_nlp.vocab, attr="LOWER")
        pattern = [self.indonesia_nlp.make_doc(s) for s in self.skills_list]
        self.matcher.add("skills", pattern)
        
    def extract(self, raw_data: str):
        raw = raw_data.lower()
        raw_doc = self.indonesia_nlp(raw)
        
        skills = []
        for match_id, start, end in self.matcher(raw_doc):
            skills.append(str(raw_doc[start:end]).strip())
        
        skills_unique = list(dict.fromkeys(skills))
        return skills_unique

