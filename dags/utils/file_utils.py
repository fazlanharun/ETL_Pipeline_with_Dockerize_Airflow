
def find_latest_csv(pattern="*sales_*.csv", search_path="."):
    import os
    import fnmatch
    
    candidates = []

    for root,dirs,files in os.walk(search_path):
        for name in fnmatch.filter(files,pattern):
            full_path = os.path.join(root,name)
            candidates.append((os.path.getmtime(full_path), full_path)) # (timestamp, path)

    if not candidates:
        raise FileNotFoundError(f"No files matching pattern '{pattern}' not found under {search_path}")
    
    latest = max(candidates)[1] #file with latest modified time
    return latest