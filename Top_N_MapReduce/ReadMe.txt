This is a MapReduce implementation of the 'Top N' algorithm. It finds top 'N' items based on their corresponding value.

This algorithm expects 3 arguments:

1. N i.e. the 'N' part of the 'Top N' algorithm
2. The input path
3. The output path

The input provided in this example is just a csv file with two comma-separated values which are the item and its value respectively. The top 'N' items here are found based on their aggregated values.

NOTE: In the example output, I have set n = 5