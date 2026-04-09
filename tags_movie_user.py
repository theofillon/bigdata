# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsPerMovieUser(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.split(',', 2)
            if len(parts) >= 2:
                user_id = parts[0]
                movie_id = parts[1]
                
                if user_id != 'userId':
                    composite_key = movie_id + "_" + user_id
                    yield composite_key, 1
        except Exception:
            pass

    def reducer(self, composite_key, counts):
        yield composite_key, sum(counts)

if __name__ == '__main__':
    TagsPerMovieUser.run()
