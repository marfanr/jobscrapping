import asyncio
from .db import Silvers

class Insight:
    def __init__(self, config: dict):
        self.silvers = Silvers(self.config)
    
    def run(self):
        try:
            print("Insight run")
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("Insight stopped.")

    async def run_async(self):
        pass