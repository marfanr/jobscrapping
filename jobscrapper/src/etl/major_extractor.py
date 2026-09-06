from pandas import DataFrame
from spacy.lang.id import Indonesian
from spacy.matcher import PhraseMatcher

class MajorExtractor:
    def __init__(self, majors_list : DataFrame):
        self.majors_list = majors_list['majors'].to_list()
        self.indonesia_nlp = Indonesian()
        self.matcher = PhraseMatcher(self.indonesia_nlp.vocab, attr="LOWER")
        pattern = [self.indonesia_nlp.make_doc(s) for s in self.majors_list]
        self.matcher.add("majors", pattern)
        
    def extract(self, raw_data: str):
        raw = raw_data.lower()
        raw_doc = self.indonesia_nlp(raw)
        
        majors = []
        for match_id, start, end in self.matcher(raw_doc):
            majors.append(str(raw_doc[start:end]).strip())
        
        majors_unique = list(dict.fromkeys(majors))
        return majors_unique

