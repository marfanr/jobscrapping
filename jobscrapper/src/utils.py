from playwright.async_api import Locator

async def get_text(l: Locator) -> str | None:
    if l is None:
        return None
    
    if await l.count() > 0:
        return await l.first.inner_text()
    
    return None

def build_query_placeholder(data):
    values = ", ".join(["(%s)"] * len(data))
    query = (
        f"VALUES {values}"
        if data
        else "SELECT NULL::TEXT WHERE FALSE"
    )
    return query