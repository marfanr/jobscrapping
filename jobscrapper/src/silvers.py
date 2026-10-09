import psycopg
from datetime import datetime
from .utils import build_query_placeholder

class Silvers:
    def __init__(self, config: dict):
        db = config.get('database')
        if not db:
            raise ValueError("database config is missing!")
        
        self.con = psycopg.connect(
            dbname=db["dbname"],
            user=db["username"],
            password=db["password"],
            host=db["host"],
            port=db["port"]
        )
        print("Connected to database")
    
    def insert(
        self,
        company: str,
        skills: list,
        majors: list,
        salary: list,
        name: str,
        url: str,
        details: str,
        source: str,
        requirements: str,
        location: str,
        listing_date: datetime,
        benefits: list,
        highlights: list,
        keyword: str
    ):
        # Normalize lists and remove duplicates
        skills = list(dict.fromkeys(skills or []))
        majors = list(dict.fromkeys(majors or []))
        benefits = list(dict.fromkeys(benefits or []))
        highlights = list(dict.fromkeys(highlights or []))

        if len(salary) != 2:
            raise ValueError("salary must contain [lower_range, upper_range]")

        skills_query = build_query_placeholder(skills)
        majors_query = build_query_placeholder(majors)
        benefits_query = build_query_placeholder(benefits)
        highlights_query = build_query_placeholder(highlights)

        query = f"""
            WITH new_skills(skill) AS (
                {skills_query}
            ),
            new_salary(low_range, upper_range) AS (
                VALUES (%s, %s)
            ),
            new_major(major) AS (
                {majors_query}
            ),
            new_benefit(benefit) AS (
                {benefits_query}
            ),
            new_highlights(highlights) AS (
                {highlights_query}
            ),
            new_role(role) AS (
                VALUES (%s)
            ),
            skills_insert AS (
                INSERT INTO silvers.skills (skill)
                SELECT skill FROM new_skills
                ON CONFLICT (skill) DO NOTHING
                RETURNING id, skill
            ),
            skills AS (
                SELECT DISTINCT s.id, s.skill
                FROM new_skills ns
                JOIN silvers.skills s USING (skill)
            ),
            majors_insert AS (
                INSERT INTO silvers.majors (major)
                SELECT major FROM new_major
                ON CONFLICT (major) DO NOTHING
                RETURNING id, major
            ),
            majors AS (
                SELECT DISTINCT m.id, m.major
                FROM new_major nm
                JOIN silvers.majors m USING (major)
            ),
            job AS (
                INSERT INTO silvers.jobs (
                    company_name,
                    job_name,
                    url,
                    details,
                    source,
                    listing_date,
                    requirements,
                    location
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, url) DO UPDATE
                    SET url = EXCLUDED.url
                RETURNING id
            ),
            job_skill AS (
                INSERT INTO silvers.job_skills (job_id, skills_id)
                SELECT j.id, s.id
                FROM job j
                CROSS JOIN skills s
                ON CONFLICT (job_id, skills_id) DO NOTHING
            ),
            job_majors AS (
                INSERT INTO silvers.job_majors (job_id, majors_id)
                SELECT j.id, m.id
                FROM job j
                CROSS JOIN majors m
                ON CONFLICT (job_id, majors_id) DO NOTHING
            ),
            benefits_insert AS (
                INSERT INTO silvers.benefits (benefit)
                SELECT benefit FROM new_benefit
                ON CONFLICT (benefit) DO NOTHING
                RETURNING id, benefit
            ),
            benefits AS (
                SELECT DISTINCT b.id, b.benefit
                FROM new_benefit nb
                JOIN silvers.benefits b USING (benefit)
            ),
            job_benefits AS (
                INSERT INTO silvers.job_benefits (job_id, benefits_id)
                SELECT j.id, b.id
                FROM job j
                CROSS JOIN benefits b
                ON CONFLICT (job_id, benefits_id) DO NOTHING
            ),
            highlights_insert AS (
                INSERT INTO silvers.highlights (highlights)
                SELECT highlights FROM new_highlights
                ON CONFLICT (highlights) DO NOTHING
                RETURNING id, highlights
            ),
            highlights AS (
                SELECT DISTINCT h.id, h.highlights
                FROM new_highlights nh
                JOIN silvers.highlights h USING (highlights)
            ),
            job_highlights AS (
                INSERT INTO silvers.job_highlights (job_id, highlights_id)
                SELECT j.id, h.id
                FROM job j
                CROSS JOIN highlights h
                ON CONFLICT (job_id, highlights_id) DO NOTHING
            ),
            roles_insert AS (
                INSERT INTO silvers.roles (role)
                SELECT role FROM new_role
                ON CONFLICT (role) DO NOTHING
                RETURNING id, role
            ),
            roles AS (
                SELECT DISTINCT r.id, r.role
                FROM new_role nr
                JOIN silvers.roles r USING (role)
            ),
            job_roles AS (
                INSERT INTO silvers.job_roles (job_id, role_id)
                SELECT j.id, r.id
                FROM job j
                CROSS JOIN roles r
                ON CONFLICT (job_id, role_id) DO NOTHING
            )
            INSERT INTO silvers.job_salary (job_id, lower_range, upper_range)
            SELECT j.id, s.low_range, s.upper_range
            FROM job j
            CROSS JOIN new_salary s
            ON CONFLICT (job_id) DO NOTHING;
        """

        params = [
            *skills,
            salary[0],
            salary[1],
            *majors,
            *benefits,
            *highlights,
            keyword,
            company,
            name,
            url,
            details,
            source,
            listing_date,
            requirements,
            location,
        ]

        try:
            with self.con.transaction():
                self.con.execute(query, params)
                return True
        except Exception as e:
            print(f"Database insert error: {e}")
            raise