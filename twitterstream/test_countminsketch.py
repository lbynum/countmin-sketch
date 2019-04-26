import pytest

from countminsketch import CountMinSketch

def test_compute_width_depth():
    delta = 0.1
    epsilon = 0.1
    depth, width = CountMinSketch.compute_depth_width(delta, epsilon)

    assert width == 2
    assert depth == 20

def test_increment_and_estimate():
    word1 = 'hello'
    word2 = 'world'
    word3 = 'other'
    countmin = CountMinSketch(0.1, 0.1)
    countmin.increment(word1)
    countmin.increment(word2)
    countmin.increment(word2)

    assert countmin.estimate(word3) == 0
    assert countmin.estimate(word1) == 1
    assert countmin.estimate(word2) == 2

    top_10_list = [('hello', 1.0), ('world', 2.0)]
    assert set(countmin.top_10_dict.items()) == set(top_10_list)


def test_merge():
    countmin1 = CountMinSketch(0.1, 0.1)
    countmin2 = CountMinSketch(0.1, 0.1)
    word = 'hello'
    countmin1.increment(word)
    countmin2.increment(word)
    countmin1.merge(countmin2)

    assert countmin2.estimate(word) == 1
    assert countmin1.estimate(word) == 2

    top_10_list = [('hello', 2.0)]
    assert set(countmin1.top_10_dict.items()) == set(top_10_list)
