import json
import os
import sys

# Mocking the comparison logic found in agents

def test_comparison(scraped_date, scraped_value, existing_details, event_name):
    existing_event = existing_details.get(event_name, {})
    existing_date = existing_event.get("date")
    existing_val = existing_event.get("value")
    
    is_changed = str(scraped_date) != str(existing_date) or str(scraped_value) != str(existing_val)
    return is_changed

# Test Case 1: Data is same
scraped_eta = "2024-03-20"
scraped_val = "Arrival"
status_details = {
    "ETA": {"date": "2024-03-20", "value": "Arrival"}
}
print(f"Test 1 (Same): {test_comparison(scraped_eta, scraped_val, status_details, 'ETA')} (Expected: False)")

# Test Case 2: Date changed
scraped_eta = "2024-03-21"
print(f"Test 2 (Date Change): {test_comparison(scraped_eta, scraped_val, status_details, 'ETA')} (Expected: True)")

# Test Case 3: Value changed
scraped_val = "Delayed"
print(f"Test 3 (Value Change): {test_comparison('2024-03-20', scraped_val, status_details, 'ETA')} (Expected: True)")

# Test Case 4: No existing data
print(f"Test 4 (New Data): {test_comparison('2024-03-20', 'Arrival', {}, 'ETA')} (Expected: True)")
