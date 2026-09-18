from ..loader import registerTask
from psycopg import Connection

@registerTask("job-distribution")
class JobDistribution:
    def __init__(self, con: Connection):
        self.con = con
    
    async def run(self):
        query = """
            WITH provinces AS (
                SELECT id as province_id, province FROM gold.dim_province
            ), matched AS (
                SELECT
                    j.id AS job_id, p.province_id,
                    row_number() over (
                        partition by j.id order by LENGTH(p.province) DESC
                        ) AS rn
                    FROM silvers.jobs j
                    JOIN provinces p
                    ON j.location LIKE '%' || p.province || '%'
            )
            INSERT INTO gold.jobs_distribution (province_id, count)
            SELECT
                province_id, count(*)
            FROM matched
            WHERE rn = 1
            GROUP BY province_id
            ON CONFLICT (province_id) DO UPDATE
                SET count = excluded.count;
            """
            
        with self.con.transaction():
            res = self.con.execute(query)
            print(f"affected {res.rowcount} rows")