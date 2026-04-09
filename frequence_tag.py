# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagFrequency(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.strip().split(',')
            
            if len(parts) >= 4:
                tag = ",".join(parts[2:-1])
                
                if tag != 'tag':
                    yield tag.lower(), 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagFrequency.run()
