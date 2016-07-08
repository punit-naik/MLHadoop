Iris database has been used as the input.

In this example, 3 nearest neighbours are used in the map phase. In the reduce phase, the dominant class is selected from all the classes sent by the mappers.

If the data size is very large, you can disable the reduction proces by setting the number of reduce tasks to zero and then run a new MapReduce job on the data which was the input to the reducer of the old job and then include the same logic, which was used in the reducer of the old job, in the mapper and reducer of the new job to find the dominant class of the input.

This algorithms takes in 4 arguments as follows:

1. The input file waiting to be classified (features separated by whitespaces in a single file)
2. The name of the input entity to be classified
3. The input (training) dataset
4. The output
