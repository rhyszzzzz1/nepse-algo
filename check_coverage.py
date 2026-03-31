import sqlite3

conn = sqlite3.connect('data/nepse.db')

# Get all active symbols
all_symbols = {r[0] for r in conn.execute("SELECT symbol FROM instruments").fetchall()}
print(f"Total symbols: {len(all_symbols)}")
print()

# Check coverage in each table
tables_to_check = [
    ('companies', 'symbol'),
    ('about_company', 'symbol'),
    ('price_history', 'symbol'),
    ('daily_price', 'symbol'),
    ('dividend', 'symbol'),
    ('news', 'symbol'),
    ('broker_summary', 'symbol'),
    ('floor_sheet', 'symbol'),
]

coverage = {}
for table, col in tables_to_check:
    try:
        count = conn.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table}").fetchone()[0]
        coverage[table] = count
    except Exception as e:
        coverage[table] = 0

print("Data coverage by table:")
for table, count in sorted(coverage.items(), key=lambda x: -x[1]):
    pct = (count / len(all_symbols) * 100) if all_symbols else 0
    print(f"  {table:20} {count:3} symbols ({pct:5.1f}%)")

print()
print("=== Symbols with NO data in any table ===")
symbols_with_data = set()
for table, col in tables_to_check:
    try:
        syms = {r[0] for r in conn.execute(f"SELECT DISTINCT {col} FROM {table}").fetchall()}
        symbols_with_data.update(syms)
    except:
        pass

no_data = sorted(all_symbols - symbols_with_data)
print(f"Found {len(no_data)} symbols with no data:")
for sym in no_data[:50]:
    print(f"  {sym}")
if len(no_data) > 50:
    print(f"  ... and {len(no_data) - 50} more")

print()
print("=== Symbols by data completeness ===")
symbol_coverage = {}
for sym in all_symbols:
    tables_with_data = 0
    for table, col in tables_to_check:
        try:
            count = conn.execute(f"SELECT 1 FROM {table} WHERE {col}=? LIMIT 1", (sym,)).fetchone()
            if count:
                tables_with_data += 1
        except:
            pass
    symbol_coverage[sym] = tables_with_data

# Group by coverage level
by_coverage = {}
for sym, count in symbol_coverage.items():
    if count not in by_coverage:
        by_coverage[count] = []
    by_coverage[count].append(sym)

for coverage_level in sorted(by_coverage.keys(), reverse=True):
    syms = by_coverage[coverage_level]
    print(f"  {coverage_level} tables: {len(syms)} symbols")
    if len(syms) <= 10:
        for s in sorted(syms):
            print(f"    - {s}")
    else:
        for s in sorted(syms)[:5]:
            print(f"    - {s}")
        print(f"    ... and {len(syms) - 5} more")

conn.close()
