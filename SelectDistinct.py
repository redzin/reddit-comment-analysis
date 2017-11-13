import sqlite3
import sys
import re
import operator
import time

def main():
t0 = time.time()
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    
    q = """
        SELECT DISTINCT subreddit_id 
        FROM comments
        """
    
    c.execute(q)
    batch_size = 10
    results = []
    indices = {}
    index = 0
    while True:
        subreddit_ids = c.fetchmany(batch_size)
        if not subreddit_ids: break
        for subreddit_id in subreddit_ids:
            if not subreddit_id: break
            if subreddit_id[0] == "t": continue
            results.append([subreddit_id[0],set()])
            indices[subreddit_id[0]] = index
            index = index + 1

    t1 = time.time()
    print(t1-t0)
    return

if __name__ == "__main__": main()