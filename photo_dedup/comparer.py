# comparer.py
from db import get_connection

def find_exact_duplicates():
    conn = get_connection()
    cursor = conn.cursor()

    rows = cursor.execute("""
        SELECT file_hash, GROUP_CONCAT(path) 
        FROM files 
        WHERE file_hash IS NOT NULL
        GROUP BY file_hash
        HAVING COUNT(*) > 1;
    """)

    groups = []
    for file_hash, paths in rows:
        groups.append(paths.split(","))
    return groups


def find_visual_duplicates(max_distance=5):
    conn = get_connection()
    cursor = conn.cursor()
    rows = cursor.execute("SELECT path, phash FROM files WHERE phash IS NOT NULL")

    # convert to list
    items = list(rows)
    groups = []

    used = set()
    for i, (p1, h1) in enumerate(items):
        if i in used:
            continue
        group = [p1]
        for j in range(i + 1, len(items)):
            p2, h2 = items[j]
            if hamming(h1, h2) <= max_distance:
                group.append(p2)
                used.add(j)
        if len(group) > 1:
            groups.append(group)

    return groups


def hamming(h1, h2):
    return sum(ch1 != ch2 for ch1, ch2 in zip(h1, h2))
