from playwright.async_api import Locator

async def get_text(l: Locator) -> str | None:
    if l is None:
        return None
    
    if await l.count() > 0:
        return await l.first.inner_text()
    
    return None