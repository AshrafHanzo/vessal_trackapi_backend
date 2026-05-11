import json
import time
import sys
from playwright.sync_api import sync_playwright

def record_mouse():
    with sync_playwright() as p:
        # Use real Chrome and disable automation flags
        browser = p.chromium.launch(
            headless=False,
            channel="chrome", # Try to use installed Chrome
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("Navigating to Hapag-Lloyd...", file=sys.stderr)
        page.goto("https://www.hapag-lloyd.com/en/online-business/track/track-by-container-solution.html")

        # Inject recording script
        page.evaluate("""
            window.recordedPath = [];
            document.addEventListener('mousemove', (e) => {
                window.recordedPath.push({
                    x: e.clientX, 
                    y: e.clientY, 
                    time: Date.now()
                });
            });
            document.addEventListener('click', (e) => {
                 window.recordedPath.push({
                    x: e.clientX, 
                    y: e.clientY, 
                    type: 'click',
                    time: Date.now()
                });
            });
            console.log("Recording started...");
        """)

        print("🔴 RECORDING STARTED! 🔴", file=sys.stderr)
        print("Please move your mouse and SOLVE the Cloudflare check naturally.", file=sys.stderr)
        print("You have 30 seconds...", file=sys.stderr)
        
        for i in range(30):
            print(f"Time remaining: {30-i}s", file=sys.stderr)
            time.sleep(1)

        # Retrieve data
        data = page.evaluate("window.recordedPath")
        
        print(f"Captured {len(data)} events.", file=sys.stderr)
        
        with open('mouse_pattern.json', 'w') as f:
            json.dump(data, f)
            
        print("Saved to mouse_pattern.json", file=sys.stderr)
        browser.close()

if __name__ == "__main__":
    record_mouse()
