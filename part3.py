
import sqlite3
import time


conn = sqlite3.connect('reddit.db')
c = conn.cursor()

q ="""
SELECT DISTINCT subreddit_id FROM comments
"""

t1 = time.time()

print("Fethcing subreddit IDs...")

result = c.execute(q)

i = 1
s_ids = list()
for row in result:
    s_ids.append(row[0])

t2 = time.time()
print("Subreddit IDs fetched. Time: ", t2-t1, ' seconds')
print("Computing average thread depth for each subreddit...")

deepest_10 = list(['', 0] for i in range(10))
for i in s_ids:
    q2 = """SELECT AVG(depth) FROM (
                WITH RECURSIVE node_ancestors(node_id, parent_id) AS (
                        SELECT id, id FROM comments WHERE subreddit_id="%s"
                    UNION ALL
                        SELECT na.node_id, comments.parent_id
                            FROM node_ancestors AS na, comments
                            WHERE comments.id = na.parent_id AND comments.parent_id IS NOT NULL
                )
                SELECT node_id, COUNT(parent_id) AS depth FROM node_ancestors GROUP BY node_id ORDER BY depth DESC   
            );
            """ % (i)
    for j in c.execute(q2):
        #for k in range(len(deepest_10)):
        #    if j[0] > deepest_10[k][1]:
        #        deepest_10.insert(k, [i,j[0]])
        #        break
        #deepest_10 = deepest_10[0:10]
        if sum([j[0] > k[1] for k in deepest_10]) > 0:
            deepest_10.append([i,j[0]])
            deepest_10 = sorted(deepest_10, key=lambda x: x[1], reverse = True)[0:10]
    
print("\r\nThe 10 subreddit ids with largest average comment thread deapth are:")
n = 1
for i in deepest_10:
    q3 = 'SELECT name FROM subreddits WHERE id="%s"' % (i[0])
    for j in c.execute(q3):
        print(n, '\t', j[0], '\t', i[1])
    n += 1


t2 = time.time()
print("\r\nTotal run time: ", t2-t1, 'seconds')
