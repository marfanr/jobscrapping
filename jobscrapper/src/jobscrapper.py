import asyncio
import json
from playwright.async_api import async_playwright
from .loader import getJobs
from collections.abc import Coroutine
from kafka import KafkaProducer, JsonSerializer
from playwright_stealth import Stealth

class Jobscrapper:
    def __init__(self, config_path: str):
        with open(config_path, "r") as file:
            self.config = json.load(file)

        self.playw = None
        self.browser = None
        self.context = None
        self.bg_task : set[asyncio.Task] = set()
        
        bs_server = self.config["kafka_bootstrap"] if self.config["kafka_bootstrap"] != None else "localhost:9092"
        print(f"kafka server: {bs_server}")
        self.producer = KafkaProducer(bootstrap_servers=bs_server,
                                      value_serializer=JsonSerializer(),
                                      )

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("Scraper stopped.")

    async def run_async(self):
        playw = self.playw = await async_playwright().start()
        stealth = Stealth()
        self.playw = stealth.use_async(self.playw)

        try:
            browser_path = self.config.get("browser_path")
            if not browser_path:
                browser_path = playw.chromium.executable_path
                
            self.browser = await playw.chromium.launch(
                executable_path=browser_path,
                headless=True,
                args=['--start-minimized'],
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
            )            

            jobs = getJobs()
            for _, v in jobs.items():
                try:
                    cls = v(self)
                    self.spawn(cls.execute())
        
                except Exception as e:
                    print(f"[JOB ERROR] {e}")

            await asyncio.gather(*self.bg_task)

        except asyncio.CancelledError:
            for task in self.bg_task:
                task.cancel()
                
            await asyncio.gather(*self.bg_task, return_exceptions=True)
            raise 
        
        except KeyboardInterrupt:
            await self._cleanup()

        finally:
            await self._cleanup()
    
    def spawn(self, task: Coroutine[any, any]):
        task_ =  asyncio.create_task(task)
        self.bg_task.add(task_)
    
    async def _cleanup(self):
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
            finally:
                self.context = None

        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass
            finally:
                self.browser = None

        if self.playw:
            try:
                await self.playw.stop()
            except Exception:
                pass
            finally:
                self.playw = None