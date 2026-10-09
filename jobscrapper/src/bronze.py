import boto3
import datetime
import json
from uuid import uuid4
from zoneinfo import ZoneInfo

class Bronze:
    def __init__(self, config: dict):
        self.config = config
        self.s3 = boto3.client(
            "s3", 
            endpoint_url="http://localhost:8333",
            aws_access_key_id="any",
            aws_secret_access_key="any",
            region_name="us-east-1"
        )
    
    def save_raw(self, data: object, portal: str, keyword: str):
        timezone = self.config["timezone"] or "Asia/Jakarta"
        scraped_at = datetime.datetime.now(ZoneInfo(timezone))
        key = (
            f"{portal}/"
            f"{keyword}/"
            f"year={scraped_at:%Y}/"
            f"month={scraped_at:%m}/"
            f"day={scraped_at:%d}/"
            f"hour={scraped_at:%H}/"
            f"{scraped_at:%Y%m%dT%H%M%SZ}_{uuid4().hex}.json"
        )

        self.s3.put_object(
            Bucket="rawjobs",
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json"
        )