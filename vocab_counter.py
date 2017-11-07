
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import sqlite3

import re

class Vocab(MRJob):
    
    # configuration for database setup
    def configure_options(self):
        super(Vocab, self).configure_options()
        self.add_file_option('--database')
        #self.SORT_VALUES = True
        
    def mapper_init(self):
        # make sqlite3 database available to mapper
        self.sqlite_conn = sqlite3.connect(self.options.database)
    
    # NOTE: The mapper runs for EACH LINE in the dummy text file
    # Use dummy text file with 1 char on a single line to avoid duplicate outputs!
    # Otherwise, the SQL query will be made once per line, resulting in duplicates
    def mapper(self, _, line):
        c = self.sqlite_conn.cursor()
        q = """
        SELECT name, body
        FROM comments INNER JOIN subreddits
        ON comments.subreddit_id=subreddits.id
        LIMIT 10"""
        q = "SELECT subreddit_id, body FROM comments LIMIT 10"
        
        # Select the subreddit id and body of each comment
        for node in c.execute(q):
            
            # Sanitize the body text
            out = node[1]
            out = out.lower()
            out = re.sub('[^\-a-z0-9\s]+', '', out)
            out = re.sub(r"[\s\-]+", " ", out, flags = re.MULTILINE)
            
            # Yield element as key=subreddit_id and value=sanitized body
            yield(node[0], out)
    
    def reducer(self, key, value):
        yield(key, list(set(' '.join(value).split())))
    
    #def reducer(self, key, value):
    #    out = list() # prepare output list
    #    # iterate over each comment mapped to the current key=subreddit_id
    #    for comment in value:
    #        out.extend(comment.split()) # put every word from any comment into same list
    #    yield (len(set(out)), key)
    
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer
            )]
    
    
if __name__ == '__main__':
    Vocab.run()


