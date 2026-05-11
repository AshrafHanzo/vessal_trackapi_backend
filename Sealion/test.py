import requests
import json
import os

# OpenAI API Key
OPEN_API_KEY = os.environ.get("OPENAI_API_KEY", "")

def test_gpt():
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPEN_API_KEY}"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "user", "content": "hi"}
        ],
        "temperature": 0.0
    }
    
    try:
        print("Sending 'hi' to GPT...")
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            print("SUCCESS!")
            print("Response:", response.json()['choices'][0]['message']['content'])
        else:
            print(f"FAILED with status code: {response.status_code}")
            print("Response Details:", response.text)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_gpt()
