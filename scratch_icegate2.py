import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto("https://foservices.icegate.gov.in/#/public-enquiries/document-status/sea-igm")
        
        # Wait for any select or input that might contain ports
        await page.wait_for_timeout(5000)
        
        # The dropdown might be a multiselect or select. Let's dump all text that looks like INCOK
        html = await page.content()
        with open("icegate_html.html", "w", encoding="utf-8") as f:
            f.write(html)
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
