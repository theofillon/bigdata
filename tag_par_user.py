# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsPerUser(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.split(',', 2)
            if len(parts) >= 2:
                user_id = parts[0]
                if user_id != 'userId':  # Ignorer le header
                    yield user_id, 1
        except Exception:
            pass

    def reducer(self, user_id, counts):
        yield user_id, sum(counts)

if __name__ == '__main__':
    TagsPerUser.run()
