from ..loader import registerTask
from psycopg import Connection

@registerTask("skill-distribution")
class SkillDistribution:
    def __init__(self, con: Connection):
        self.con = con
    
    async def run(self):
        query = """
            WITH provinces AS (
                SELECT id AS province_id, province FROM gold.dim_province
            ),
            matched AS (
                SELECT
                    j.id AS job_id,
                    p.province_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY j.id
                        ORDER BY LENGTH(p.province) DESC
                        ) AS rn
                FROM silvers.jobs j
                        JOIN provinces p
                            ON j.location LIKE '%' || p.province || '%'
            )
            INSERT INTO gold.skills_distribution (province_id, skill, count)
            SELECT
                m.province_id,
                s.skill,
                COUNT(*) AS job_count
            FROM matched m
                JOIN silvers.job_skills js
                    ON js.job_id = m.job_id
                JOIN silvers.skills s
                    ON s.id = js.skills_id
            WHERE m.rn = 1
            GROUP BY
                m.province_id,
                s.id,
                s.skill
            ORDER BY
                m.province_id,
                job_count DESC
            ON CONFLICT (province_id, skill) DO UPDATE
                SET count = EXCLUDED.count
            ;
            """
            
        with self.con.transaction():
            res = self.con.execute(query)
            print(f"affected {res.rowcount} rows")