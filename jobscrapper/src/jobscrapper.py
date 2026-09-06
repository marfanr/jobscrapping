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
        self.bg_task: set[asyncio.Task] = set()
        
        bs_server = self.config.get("kafka_bootstrap") or "localhost:9092"
        print(f"kafka server: {bs_server}")
        self.producer = KafkaProducer(
            bootstrap_servers=bs_server,
            value_serializer=JsonSerializer(),
        )

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("Scraper stopped.")
        finally:
            self.producer.close()

    async def run_async(self):
        playw = await async_playwright().start()
        stealth = Stealth()
        self.playw = stealth.use_async(playw)

        try:
            browser_path = self.config.get("browser_path") or playw.chromium.executable_path
                
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

            # Wait with graceful timeout
            await self._wait_tasks(timeout=300)  # 5 min graceful wait

        except KeyboardInterrupt:
            print("Shutdown signal received, cleaning up...")
            await self._cleanup(timeout=30, force_after=True)
        finally:
            await self._cleanup(timeout=30, force_after=False)

    async def _wait_tasks(self, timeout=300):
        """Wait for all tasks to complete gracefully"""
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.bg_task, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            print(f"Tasks timeout after {timeout}s, forcing cancel...")
            await self._cleanup(timeout=10, force_after=True)
    
    def spawn(self, task: Coroutine):
        task_obj = asyncio.create_task(task)
        self.bg_task.add(task_obj)
        task_obj.add_done_callback(self.bg_task.discard)

    async def _cleanup(self, timeout=30, force_after=True):
        """Cleanup with graceful shutdown then force if needed"""
        
        for task in self.bg_task:
            if not task.done():
                task.cancel()
        
        # Give tasks time to cleanup (finally blocks, close pages, etc)
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.bg_task, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            if force_after:
                print("Tasks still running after timeout, force closing...")
            else:
                print(f"Warning: {len([t for t in self.bg_task if not t.done()])} tasks still running")
        
        self.bg_task.clear()

        # Cleanup Playwright in reverse order
        if self.context:
            try:
                await asyncio.wait_for(self.context.close(), timeout=10)
            except Exception as e:
                print(f"[CLEANUP] Context error: {e}")
            finally:
                self.context = None

        if self.browser:
            try:
                await asyncio.wait_for(self.browser.close(), timeout=10)
            except Exception as e:
                print(f"[CLEANUP] Browser error: {e}")
            finally:
                self.browser = None

        if hasattr(self, 'playw_original') and self.playw_original:
            try:
                await asyncio.wait_for(self.playw_original.stop(), timeout=10)
            except Exception as e:
                print(f"[CLEANUP] Playwright error: {e}")
            finally:
                self.playw_original = None
                self.playw = None
        
        await asyncio.sleep(0.1)