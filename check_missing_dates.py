#!/usr/bin/env python3
"""
Detailed analysis: Show exactly which dates are MISSING for each symbol.
Focuses on critical symbols with high missing percentages.
"""

import sqlite3
from datetime import datetime, timedelta
import json

DB_PATH = "data/nepse.db"

def get_expected_dates():
    """Get all expected trading dates in the range."""
    start = datetime(2025, 3, 17)
    end = datetime(2026, 3, 18)
    
    expected = set()
    current = start
    while current <= end:
        if current.weekday() < 5:
            expected.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return sorted(list(expected))

def check_missing_dates():
    """Check which dates are missing for each symbol."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT symbol FROM broker_summary ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    
    expected_dates = get_expected_dates()
    expected_set = set(expected_dates)
    
    # Filter to only critical symbols (>10% missing)
    critical_data = []
    
    for symbol in symbols:
        cursor.execute(
            "SELECT DISTINCT date FROM broker_summary WHERE symbol = ? ORDER BY date",
            (symbol,)
        )
        actual_dates = {row[0] for row in cursor.fetchall()}
        missing_dates = expected_set - actual_dates
        missing_pct = (len(missing_dates) / len(expected_dates)) * 100
        
        if missing_pct > 10:  # Only critical (>10% missing)
            critical_data.append({
                'symbol': symbol,
                'missing_count': len(missing_dates),
                'missing_pct': missing_pct,
                'missing_dates': sorted(list(missing_dates))
            })
    
    conn.close()
    
    # Sort by missing count (most critical first)
    critical_data.sort(key=lambda x: x['missing_count'], reverse=True)
    
    print(f"🔴 CRITICAL: {len(critical_data)} symbols with >10% missing data\n")
    print("="*100)
    
    for item in critical_data[:20]:  # Show top 20
        symbol = item['symbol']
        missing_count = item['missing_count']
        missing_pct = item['missing_pct']
        missing_dates = item['missing_dates']
        
        # Find first and last missing dates
        first_missing = missing_dates[0]
        last_missing = missing_dates[-1]
        
        print(f"\n{symbol}")
        print(f"  Missing: {missing_count}/{len(expected_dates)} dates ({missing_pct:.1f}%)")
        print(f"  Range: {first_missing} to {last_missing}")
        print(f"  Sample missing dates (first 10):")
        for date in missing_dates[:10]:
            print(f"    - {date}")
        if len(missing_dates) > 10:
            print(f"    ... and {len(missing_dates) - 10} more")
    
    print("\n" + "="*100)
    print(f"\nTotal critical symbols: {len(critical_data)}")
    
    # Save full data to JSON for reference
    with open('missing_dates_detail.json', 'w') as f:
        json.dump(critical_data, f, indent=2)
    print(f"Full details saved to: missing_dates_detail.json")
    
    # Create a summary showing date ranges with most failures
    print("\n" + "="*100)
    print("DATES WITH MOST FAILURES (top 20):")
    print("="*100)
    
    date_failure_count = {}
    for item in critical_data:
        for date in item['missing_dates']:
            date_failure_count[date] = date_failure_count.get(date, 0) + 1
    
    sorted_dates = sorted(date_failure_count.items(), key=lambda x: x[1], reverse=True)
    for date, count in sorted_dates[:20]:
        print(f"{date}: {count} symbols missing this date")

if __name__ == "__main__":
    check_missing_dates()
