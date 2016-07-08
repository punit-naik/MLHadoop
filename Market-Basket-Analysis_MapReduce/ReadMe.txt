This is a MapReduce implementation of one of the simplest algorithms called Market Basket Analysis.

This algorithm helps the user to determine which items have been occuring together.
In marketing terms, this algorihtm can help the vendor (online or local) to determine which items to be clubbed together on the shelf.
For example, many customer might have bought butter along with bread. So naturally it would be a wise choice to juxtapose them on the shelf and this algorithm helps the vendors to do the same.

The sample input data contains transactions of all the customers.
It has a comma separated list of items bought by a customer.

The sample output is the frequency of occurence of groups of items.

This algorithm takes in three arguments:

1. The input path
2. The output path
3. Number of groupings i.e. How many items shoould be grouped together. Set this carefully as a the value of number of groupings should be always less than or equal to the number of items purchased by every customer.