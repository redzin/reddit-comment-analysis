
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import sqlite3

import re

class Vocab(MRJob):
    
    def configure_options(self):
        super(Vocab, self).configure_options()
        self.add_file_option('--database')
        
    def mapper_init(self):
        # make sqlite3 database available to mapper
        self.sqlite_conn = sqlite3.connect(self.options.database)
    
    def mapper(self, _, line):
        c = self.sqlite_conn.cursor()
        for node in c.execute('SELECT subreddit_id, body FROM comments LIMIT 10'):
            out = re.sub('[^A-Za-z0-9\s]+', '', node[1]) 
            out = out.lower()
            out = re.sub(r"^\s+", " ", out, flags = re.MULTILINE)
            out = ' '.join(word for word in set(out.split()))
            yield (node[0], out)

    def reducer(self, sub_reddit_id, word):
        out = list(set(list(word)))
        yield (sub_reddit_id, out)
    
    
    
    def mapper2 (self, key, line):
        yield (key, list(set(' '.join(line).split())))
    
    def reducer2 (self, key, value):
        yield (key, list(value))
    
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer
            ),MRStep(
                mapper=self.mapper2,
                reducer=self.reducer2
            )]

if __name__ == '__main__':
    Vocab.run()


