import asyncio
from .loader import getTask
import json
import psycopg

class Olap:
    def __init__(self, config_path: str):
        with open(config_path, "r") as file:
            self.config = json.load(file)
        
        db = self.config['database']
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
    
    def run(self, task_name: str):
        if task_name is None:
            raise "task name is required"
        
        try:
            print("Olap run")
            asyncio.run(self.run_async(task_name=task_name))
        except KeyboardInterrupt:
            print("Olap stopped.")

    async def run_async(self, task_name: str):        
        task_ = getTask(task_name)
        task = task_(self.con)
        await task.run()