# PageRank
This is a python script for a page rank algorithm using Spark.

- The input is taken from a command line argument. The format of running it is =>
                                          
      python pagerank.py [input-file] [no. of iterations]

- Here is where the spark implementation begins. It collects the pairs of URLS and groups them by the same key.
- The pairs are initialized to a weight of '1'.
- Now with the no. of iterations read from the CL args, we divide each value to the key it goes and add that up.
- There is small implementation to handle spider traps called "damping". in quick words, Spider Traps are dead-ends or infinite loops where the iterations get caught in a repetitive chain or cant iterate to another point or key in the algorithm. For this we've assumed the constant d. Its normally d=0.85 but I've assumed 0.80. The line of code for this is =>

      ranks = urls_joined.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
