import glob
import re

for f in glob.glob('src/**/*.py', recursive=True):
    if f.endswith('db.py'): continue
    
    try:
        with open(f, 'r', encoding='utf-8') as file:
            content = file.read()
    except Exception:
        continue
        
    if 'sqlite3' not in content and 'get_db' not in content:
        continue
    
    # Replace import sqlite3 with import + the db wrapper
    content = content.replace('import sqlite3', 'import sqlite3\nfrom db import get_db')
    
    # Handle DB_PATH imports dynamically (just remove DB_PATH symbol if it's there)
    content = re.sub(r'from config import (\(.*?)DB_PATH,\s*(.*?)\)', r'from config import \1\2)', content, flags=re.DOTALL)
    content = content.replace('from config import DB_PATH\n', '')
    content = content.replace('from config import DB_PATH,', 'from config import')
    content = content.replace(', DB_PATH', '')
    
    # Remove all def get_db() implementations
    content = re.sub(r'def _?get_db\(\):[\s\n]*?(?:os\.makedirs[^\n]*\n)?[\s\n]*?return sqlite3\.connect\([^\)]*\)\n', '', content)
    
    # Replace internal _get_db calls
    content = content.replace('_get_db()', 'get_db()')
    
    # Fix the sqlite_master calls specifically if they were missed by the wrapper
    # The wrapper does capture sqlite_master if the exact string is in the execute statement
    
    with open(f, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Patched {f}")
