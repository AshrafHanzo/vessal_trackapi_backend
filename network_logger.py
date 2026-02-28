
import sys
import json
from playwright.sync_api import sync_playwright

def log_network_traffic(container_number):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        # Capture all network requests
        requests = []
        
        def handle_response(response):
            try:
                # Log all requests/responses for debugging
                content_type = response.headers.get("content-type", "").lower()
                try:
                    body = response.text()
                except:
                    body = "<binary data>"
                
                requests.append({
                    "url": response.url,
                    "method": response.request.method,
                    "status": response.status,
                    "content_type": content_type,
                    "request_headers": response.request.headers,
                    "body": body[:5000] # Save first 5KB
                })
                sys.stderr.write(f"CAPTURED: {response.url} ({content_type})\\n")
            except Exception as e:
                pass

        try:
            page.on("response", handle_response)
            
            # Navigate and track
            page.goto("https://www.sealioncargo.com/track.html")
            page.wait_for_timeout(2000)
            
            # Wait for input field
            page.wait_for_timeout(5000)
            
            # Enter container number in shadow DOM input using robust selector
            input_eval_js = '''
                const input = document.querySelector("#tracking_system_root")
                    .shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > div > input[type=text]");
                input.value = "''' + container_number + '''";
                input.dispatchEvent(new Event('input', { bubbles: true }));
                
                // Find and click search button
                const btn = document.querySelector("#tracking_system_root")
                    .shadowRoot.querySelector("#app-root > div > div.container-tracking-VvPpX6 > div > div.container-tracking-r8H33s > div > button");
                btn.click();
            '''
            page.evaluate(input_eval_js)
            
            # Wait for results to load by checking for "Route" text in shadow DOM
            sys.stderr.write("DEBUG: Waiting for results to load...\\n")
            results_loaded = False
            for _ in range(30): # 30 seconds max
                try:
                    loaded = page.evaluate('''
                        (() => {
                            const root = document.querySelector("#tracking_system_root");
                            if (!root || !root.shadowRoot) return false;
                            return root.shadowRoot.textContent.includes("Route");
                        })()
                    ''')
                    if loaded:
                        results_loaded = True
                        sys.stderr.write("DEBUG: Results detected in UI!\\n")
                        # Wait a bit more for finishing all requests
                        page.wait_for_timeout(5000)
                        break
                except:
                    pass
                page.wait_for_timeout(1000)
            
            if not results_loaded:
                sys.stderr.write("WARNING: Results did not load in time.\\n")
        finally:
            try:
                browser.close()
            except:
                pass
            
            # Dump captured requests to file
            with open("network_log.json", "w", encoding="utf-8") as f:
                json.dump(requests, f, indent=2)
                
            print(f"Captured {len(requests)} potential API responses")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python network_logger.py <container_number>")
    else:
        log_network_traffic(sys.argv[1])
