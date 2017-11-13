import sqlite3
import sys
import re
import operator
import time

def main():
    t0 = time.time()
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    q = "SELECT subreddit_id, body FROM comments"
    c.execute(q)
    batch_size=100
    results = {}
    while True:
        rows = c.fetchmany(batch_size)
        if not rows: break
        for row in rows:
            if(row[0] not in results):
                results[row[0]] = set()
            for word in Sanitize(row[1]).split():
                results[row[0]].add(word)
    
    for v in results:
        results[v] = len(results[v])
        
    sorted_results = sorted(results.items(), key=operator.itemgetter(1), reverse=True)
    result = sorted_results[:10]
    for v in result:
        print(v)
    conn.close()
    t1 = time.time()
    print(t1-t0)
    
def Sanitize(string):
    string = string.lower()
    string = re.sub('[^\-a-z0-9\s]+', '', string)
    string = re.sub(r"[\s\-]+", " ", string, flags = re.MULTILINE)
    return string

if __name__ == "__main__": main()