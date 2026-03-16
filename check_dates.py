import sqlite3

conn = sqlite3.connect("data/nepse.db")
cursor = conn.cursor()

# Get a sample of dates
cursor.execute("SELECT DISTINCT date FROM floor_sheet ORDER BY date DESC LIMIT 20")
print("Top 20 dates descending:")
for row in cursor.fetchall():
    print(f"  '{row[0]}'")

# Get totally random sample
cursor.execute("SELECT DISTINCT date FROM floor_sheet ORDER BY RANDOM() LIMIT 10")
print("\nRandom 10 dates:")
for row in cursor.fetchall():
    print(f"  '{row[0]}'")

conn.close()
