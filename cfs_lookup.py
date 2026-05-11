"""
CFS Code Lookup — Shared Utility Module

Fetches CFS codes from https://trackcontainer.in/api/cfs-codes
and provides fuzzy matching to resolve scraped codes to CFS names.

Usage:
    from cfs_lookup import resolve_cfs_name
    
    code, name = resolve_cfs_name("INAGL1")
    # code = "INMAA1AGL1", name = "ALL CARGO LOGISTICS"
"""

import requests
import sys
import time
from difflib import SequenceMatcher

# ==========================================
# CONFIGURATION
# ==========================================
CFS_CODES_API = "https://trackcontainer.in/api/cfs-codes"
CACHE_TTL = 3600  # Re-fetch every 1 hour
MIN_FUZZY_SCORE = 0.6  # Minimum similarity score to consider a match

# ==========================================
# IN-MEMORY CACHE
# ==========================================
_cfs_cache = {
    "data": None,
    "fetched_at": 0
}


def fetch_cfs_codes():
    """Fetch CFS codes from the API. Uses in-memory cache with TTL."""
    now = time.time()
    if _cfs_cache["data"] and (now - _cfs_cache["fetched_at"]) < CACHE_TTL:
        return _cfs_cache["data"]
    
    try:
        resp = requests.get(CFS_CODES_API, timeout=15)
        resp.raise_for_status()
        result = resp.json()
        codes = result.get("data", [])
        _cfs_cache["data"] = codes
        _cfs_cache["fetched_at"] = now
        sys.stderr.write(f"CFS_LOOKUP: Fetched {len(codes)} CFS codes from API\n")
        return codes
    except Exception as e:
        sys.stderr.write(f"CFS_LOOKUP: Failed to fetch CFS codes: {e}\n")
        # Return cached data even if expired, or empty list
        return _cfs_cache["data"] or []


def resolve_cfs_name(scraped_code):
    """
    Fuzzy match a scraped CFS code against the database.
    
    Returns: (matched_cfs_code, matched_cfs_name)
    - If match found: ("INMAA1AGL1", "ALL CARGO LOGISTICS")
    - If no match: (scraped_code, None)  — always returns raw code
    
    Matching strategy (priority order):
    1. Exact match on cfs_code
    2. Scraped code is a suffix of a DB code (e.g. "AGL1" in "INMAA1AGL1")
    3. DB code ends with scraped code after removing common prefix "IN"
    4. Fuzzy ratio using SequenceMatcher
    """
    if not scraped_code:
        return (None, None)
    
    scraped_code = scraped_code.strip().upper()
    codes = fetch_cfs_codes()
    
    if not codes:
        return (scraped_code, None)
    
    # --- Strategy 1: Exact match ---
    for entry in codes:
        if entry["cfs_code"].upper() == scraped_code:
            sys.stderr.write(f"CFS_LOOKUP: EXACT match '{scraped_code}' → '{entry['cfs_name']}'\n")
            return (entry["cfs_code"], entry["cfs_name"])
    
    # --- Strategy 2: Substring/suffix match ---
    # e.g. scraped "INAGL1" → strip "IN" prefix → "AGL1" → check if DB code ends with "AGL1" 
    # Also check if DB code contains the scraped code
    scraped_clean = scraped_code
    if scraped_clean.startswith("IN"):
        scraped_clean = scraped_clean[2:]  # Remove "IN" prefix
    
    best_substring = None
    for entry in codes:
        db_code = entry["cfs_code"].upper()
        # Check if DB code ends with the cleaned scraped code
        if scraped_clean and db_code.endswith(scraped_clean):
            sys.stderr.write(f"CFS_LOOKUP: SUFFIX match '{scraped_code}' (cleaned: {scraped_clean}) → '{entry['cfs_name']}' ({entry['cfs_code']})\n")
            return (entry["cfs_code"], entry["cfs_name"])
        # Check if scraped code is contained in DB code
        if scraped_clean and scraped_clean in db_code:
            best_substring = entry
    
    if best_substring:
        sys.stderr.write(f"CFS_LOOKUP: CONTAINS match '{scraped_code}' → '{best_substring['cfs_name']}' ({best_substring['cfs_code']})\n")
        return (best_substring["cfs_code"], best_substring["cfs_name"])
    
    # --- Strategy 3: Also try matching without port prefix ---
    # DB codes are like "INMAA1XXX1", "INENR1XXX1", "INKAT1XXX1"
    # Scraped may be "INXXX1" or just "XXX1"
    port_prefixes = ["INMAA1", "INENR1", "INKAT1"]
    for entry in codes:
        db_code = entry["cfs_code"].upper()
        # Strip common port prefix from DB code
        db_suffix = db_code
        for prefix in port_prefixes:
            if db_suffix.startswith(prefix):
                db_suffix = db_suffix[len(prefix):]
                break
        
        if scraped_clean == db_suffix:
            sys.stderr.write(f"CFS_LOOKUP: PREFIX-STRIPPED match '{scraped_code}' → '{entry['cfs_name']}' ({entry['cfs_code']})\n")
            return (entry["cfs_code"], entry["cfs_name"])
    
    # --- Strategy 4: Fuzzy match ---
    best_score = 0
    best_match = None
    port_prefixes = ["INMAA1", "INENR1", "INKAT1"]
    for entry in codes:
        db_code = entry["cfs_code"].upper()
        # Compare cleaned codes
        score = SequenceMatcher(None, scraped_clean, db_code).ratio()
        if score > best_score:
            best_score = score
            best_match = entry
        # Also compare against suffix (without port prefix)
        db_suffix = db_code
        for prefix in port_prefixes:
            if db_suffix.startswith(prefix):
                db_suffix = db_suffix[len(prefix):]
                break
        score2 = SequenceMatcher(None, scraped_clean, db_suffix).ratio()
        if score2 > best_score:
            best_score = score2
            best_match = entry
    
    if best_match and best_score >= MIN_FUZZY_SCORE:
        sys.stderr.write(f"CFS_LOOKUP: FUZZY match '{scraped_code}' → '{best_match['cfs_name']}' ({best_match['cfs_code']}) score={best_score:.2f}\n")
        return (best_match["cfs_code"], best_match["cfs_name"])
    
    # --- No match found: return raw code ---
    sys.stderr.write(f"CFS_LOOKUP: NO match for '{scraped_code}', returning raw code\n")
    return (scraped_code, None)


# ==========================================
# CLI TEST
# ==========================================
if __name__ == "__main__":
    test_codes = ["INACT", "INAGL1", "INMAA1AGL1", "INMAA1KSS1", "XYZ123", "ACT", ""]
    if len(sys.argv) > 1:
        test_codes = sys.argv[1:]
    
    print(f"{'Scraped Code':<15} {'Matched Code':<15} {'CFS Name'}")
    print("-" * 55)
    for code in test_codes:
        matched_code, cfs_name = resolve_cfs_name(code)
        print(f"{code or '(empty)':<15} {matched_code or 'None':<15} {cfs_name or 'None'}")
