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
    batch_size = 100
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
            
    q = """
        SELECT DISTINCT subreddit_id, author_id
        FROM comments
        """
    c.execute(q)


    while True:
        author_ids = c.fetchmany(batch_size)
        if not author_ids: break
        for author_id in author_ids:
            if not author_id: break
            if author_id[0] not in indices: continue
            results[indices[author_id[0]]][1].add(author_id[1])

    t1 = time.time()
    print(t1-t0)
    return
            
    index = 0
    length = len(results)
    subreddit_connections = {}
    secondary = ""
    variation_one = ""
    variation_two = ""
    for main in indices:
        if main == "t":
            continue
        for j in range(index, length):
            if not results[j]: break
            if main == results[j][0]: continue
            secondary = results[j][0]
            variation_one = f'{main}+{secondary}'
            variation_two = f'{secondary}+{main}'
            if variation_one not in subreddit_connections and variation_two not in subreddit_connections:
                subreddit_connections[variation_one] = len(results[indices[main]][1] & results[j][1])
                if not subreddit_connections[variation_one]:
                    subreddit_connections[variation_one] = 0
            index = index + 1
        
    sorted_results = sorted(subreddit_connections.items(), key=operator.itemgetter(1), reverse=True)
    result = sorted_results[:10]
    if len(result) < 10 :
        for v in range(len(result)):
            print(result[v])
    else:
        for v in range(10):
            print(result[v])
        
    conn.close()
    t1 = time.time()
    print(t1-t0)

if __name__ == "__main__": main()