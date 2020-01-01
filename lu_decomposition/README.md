# LU Decomposition

This mapreduce algorithm splits massively large matrix into it's `L` and `U` components. It uses the Naive Gaussian Elimination technique to do so.

**NOTE**: Only works on square matrix inputs

# Program Execution Arguments

This programs only expects two arguments:

1. An input path
2. An output path

# Input and Output data shape

Both the input and output matrix shapes are the **SAME**. This program expects and produces the textual input of matrices in the following manner:

`row_number + "\t" + elem-1 + "," + elem-2 + "," + elem-3 ...`

The text files should be a tab-separated list of `row_number`s and comma-separated row elements

# Final Output Location

This program produces various intermediate outputs. But the actual output (`L` and `U` matrices) are present in the paths `<output_path> + "-merged/lower"` and `<output_path> + "-merged/upper"`.

It's shapes will correspond to the shapes defined above.

**NOTE**: I have provided the input and all the output (intermediate and actual) folders, you can use them to verify your outputs.

# Limitations

This program uses Naive Gaussian Elimination method as mentioned eariler which produces a lot of intermediate outputs. This is fine for large datasets but as the daaset grows (with the number of input rows), this program will produce a lot of intermediate outputs which might cause a bottleneck on the I/O.

I will think about improving the algorithm flow/process or using a different technique altogether.
