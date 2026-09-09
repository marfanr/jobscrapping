import psycopg
from datetime import datetime

class Silvers:
    def __init__(self, config: dict):
        db = config['database']
        if db is None:
            raise ValueError("database config is missing!")
        
        self.con = psycopg.connect(
            dbname=db["dbname"],
            user=db["username"],
            password=db["password"],
            host=db["host"],
            port=db["port"]
        )

        if self.con:
            print("Connected")
            
    def _build_query_placeholder(self, data):
        values = ", ".join(["(%s)"] * len(data))
        query = (
            f"VALUES {values}"
            if data
            else "SELECT NULL::TEXT WHERE FALSE"
        )
        return query
    
    def insert(
        self,
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
        benefits: list
    ):
        skills = skills or []
        majors = majors or []
        benefits = benefits or []

        if len(salary) != 2:
            raise ValueError("salary must contain [lower_range, upper_range]")

        skills_query = self._build_query_placeholder(skills)
        majors_query = self._build_query_placeholder(majors)
        benefits_query = self._build_query_placeholder(benefits)

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
            skills AS (
                INSERT INTO silvers.skills (skill)
                SELECT s.skill
                FROM new_skills s
                ON CONFLICT (skill) DO UPDATE
                    SET skill = EXCLUDED.skill
                RETURNING id
            ),
            majors AS (
                INSERT INTO silvers.majors (major)
                SELECT m.major
                FROM new_major m
                ON CONFLICT (major) DO UPDATE
                    SET major = EXCLUDED.major
                RETURNING id
            ),
            job AS (
                INSERT INTO silvers.jobs (
                    job_name,
                    url,
                    details,
                    source,
                    listing_date,
                    requirements,
                    location
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(source, url) DO UPDATE
                    SET url = EXCLUDED.url
                RETURNING id
            ),
            job_skill AS (
                INSERT INTO silvers.job_skills (
                    job_id,
                    skills_id
                )
                SELECT j.id, s.id
                FROM job j
                CROSS JOIN skills s
                ON CONFLICT (job_id, skills_id) DO NOTHING
            ),
            job_majors AS (
                INSERT INTO silvers.job_majors (
                    job_id,
                    majors_id
                )
                SELECT j.id, m.id
                FROM job j
                CROSS JOIN majors m
                ON CONFLICT (job_id, majors_id) DO NOTHING
            ),                
            benefits AS (
                INSERT INTO silvers.benefits (benefit)
                    SELECT b.benefit FROM new_benefit b
                    ON CONFLICT (benefit) DO UPDATE
                        SET benefit = excluded.benefit
                    RETURNING id
            ), job_benefits AS (
                INSERT INTO silvers.job_benefits (job_id, benefits_id)
                    SELECT j.id, b.id
                    FROM job j CROSS JOIN benefits b
                    ON CONFLICT (job_id, benefits_id) DO NOTHING
            )
            INSERT INTO silvers.job_salary (
                                job_id,
                                lower_range,
                                upper_range
                            )
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
            name,
            url,
            details,
            source,
            listing_date,
            requirements,
            location,
        ]

        with self.con.transaction():
            res = self.con.execute(query, params)
            return res