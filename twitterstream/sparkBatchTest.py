from pyspark import SparkContext, streaming

from countminsketch import CountMinSketch

def increment_countmin(countmin, item):
    ''' Increment `item` in the given CountMinSketch `countmin`.

    Parameters
    ----------
    countmin : a CountMinSketch object
    item : a hashable object

    Return
    ------
    none
    '''
    if item is not None:
        countmin.increment(item)
    return countmin

def merge_countmins(countmin1, countmin2):
    ''' Merge two CountMinSketch objects.

    Parameters
    ----------
    countmin1 : a CountMinSketch object
    countmin2 : a CountMinSketch object

    Return
    ------
    none
    '''
    countmin1.merge(countmin2)
    return countmin1

def print_top_10(countmin):
    ''' Print top 10 items in CountMinSketch.

    Parameters
    ----------
    countmin : a CountMinSketch object

    Return
    ------
    none
    '''
    sorted_top_10 = sorted(countmin.top_10_dict.items(),
                           key=lambda x: -x[1])
    for rank, (item, item_estimate) in enumerate(sorted_top_10):
        print('# ' + rank + '\t' + item + ' :\t' + item_estimate)


def get_tweets(countmin):
    ''' Generate multiple streams of twitter data using W workers and P
    partitions. Create a CountMinSketch on each of the P partitions.
    '''
    # create local StreamingContext with two working threads and batch
    #  interval of 10 seconds
    sc = SparkContext('local[2]', 'Spark Count-Min Sketch')
    sc.setLogLevel('ERROR')
    ssc = streaming.StreamingContext(sc, 1)

    # create data stream connected to hostname:port
    socket_stream = ssc.socketTextStream('127.0.0.1', 5555)
    lines = socket_stream.window(10)

    # increment each hashtag
    result = lines.flatMap(lambda line: line.split(" "))\
         .filter(lambda word: word.lower().startswith('#'))\
         .map(lambda word: increment_countmin(countmin, word))\
         .reduce(lambda countmin1, countmin2: merge_countmins(countmin1, countmin2))\
         .map(lambda new_countmin: merge_countmins(countmin, new_countmin))

    result.pprint()

    # start context
    ssc.start()
    ssc.awaitTermination(timeout=30)


if __name__ == '__main__':
    countmin = CountMinSketch(delta=0.01, epsilon=0.01)
    get_tweets(countmin)

    # show the top 10
    # print('============================================================',
    #       '\n                        TOP HITTERS                        ',
    #       '\n============================================================')
    # print_top_10(countmin)
    # print(countmin.table)
    # print('============================================================')
