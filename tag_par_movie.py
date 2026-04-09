# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class TagsPerMovie(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.split(',', 2)
            if len(parts) >= 2:
                movie_id = parts[1]
                if movie_id != 'movieId':  # Ignorer le header
                    yield movie_id, 1
        except Exception:
            pass

    def reducer(self, movie_id, counts):
        yield movie_id, sum(counts)

if __name__ == '__main__':
    TagsPerMovie.run()
