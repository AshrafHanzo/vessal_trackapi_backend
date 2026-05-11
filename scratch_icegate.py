import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://enquiry.icegate.gov.in/enquiryatices/seaIgmEntry")
        await page.wait_for_selector("#location", state="visible")
        
        options = await page.evaluate("Array.from(document.querySelectorAll('#location option')).map(o => o.text)")
        for opt in options:
            if "COCHIN" in opt.upper() or "KOCHI" in opt.upper():
                print(opt)
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
