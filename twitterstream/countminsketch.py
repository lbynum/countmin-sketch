from math import log, floor
from collections import Counter

import mmh3
import numpy as np


class CountMinSketch:
    '''Implements the Count-Min Sketch algorithm.'''

    def __init__(self, delta, epsilon):
        depth, width = self.compute_depth_width(delta, epsilon)
        self.table_depth = depth # number of hash functions
        self.table_width = width
        self.table = np.zeros((self.table_depth, self.table_width))
        self.top_10_dict = dict()

    def __str__(self):
        top_10_string = ''
        sorted_top_10 = sorted(self.top_10_dict.items(),
                               key=lambda x: -x[1])
        for rank, (item, item_estimate) in enumerate(sorted_top_10):
            top_10_string += '# ' + str(rank) + '\t' + str(item) + ' :\t' + str(item_estimate) + '\n'

        return top_10_string

    @staticmethod
    def compute_depth_width(delta, epsilon):
        '''Use accuracy and error probability of the sketch to define the
        width and depth.

        Parameters
        ----------
        delta: accuracy, float
        epsilon: error probability, float

        Returns
        -------
        width : int
        depth : int
        '''
        depth = floor(2 / epsilon)
        width = floor(log(1.0 / delta))
        return depth, width

    def increment(self, item):
        '''Increment the corresponding counter for this item.

        Parameters
        ----------
        item : a hashable object

        Returns
        -------
        none
        '''
        if item is None:
            return
        for hash_number in range(self.table_depth):
            index = mmh3.hash(item, hash_number) % self.table_width
            self.table[hash_number][index] += 1
        self.add_to_top_10(item)

    def add_to_top_10(self, item):
        '''Add item to top ten list if estimate is high enough.

        Parameters
        ----------
        item : a hashable object

        Returns
        -------
        none
        '''
        current_item_estimate = self.estimate(item)
        if item in self.top_10_dict.keys() or len(self.top_10_dict) < 10:
            # update if there or add if there are less than 10
            self.top_10_dict[item] = current_item_estimate
        else:
            # replace minimum in top 10 if current is better
            min_top_item = min(self.top_10_dict.items(), key=lambda x: x[1])
            if current_item_estimate > min_top_item[1]:
                self.top_10_dict.pop(min_top_item[0])
                self.top_10_dict[item] = current_item_estimate

    def merge_top_10_dicts(self, other_top_10_dict):
        '''Merge other top_10_dict with current top_10_dict.

        Parameters
        ----------
        other_top_10_dict : dict

        Returns
        -------
        none
        '''
        all_items_counter = (Counter(self.top_10_dict) +
                             Counter(other_top_10_dict))
        self.top_10_dict = dict(all_items_counter.most_common(10))

    def estimate(self, item):
        '''Estimate the count for the given value.

        Parameters
        ----------
        item : a hashable object

        Returns
        -------
        int
        '''
        if item is None:
            return 0
        all_counts = []
        for hash_number in range(self.table_depth):
            index = mmh3.hash(item, hash_number) % self.table_width
            all_counts.append(self.table[hash_number][index])
        # stored counts are upper bounds for true count, thus return minimum
        return min(all_counts)


    def merge(self, other_countminsketch):
        '''Merge a CountMinSketch with the current CountMinSketch (in order
        to merge sketches across partitions).

        Parameters
        ----------
        other_countminsketch : a CountMinSketch object
        Returns
        -------
        none
        '''
        self.table += other_countminsketch.table
        self.merge_top_10_dicts(other_countminsketch.top_10_dict)
