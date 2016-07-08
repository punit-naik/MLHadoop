The input is a record of women who were diagnosed for Diabetes. Each line is an input point with the last value being the output.

In this case, the output is either 0 or 1. 0 means tested negative for Diabetes and 1 means otherwise.

This code can easily be converted into LWR (Locally Weighted Regression), a technique which discards the less critical (irrelevant) input points, by simply multiplying the weighting function which is given by;

w(i) = exp(-(X[i]-X)^2/(2T^2))		;; "exp" is "e^"

X[i] is the input point
X is the query point (The input for which you want to predict the output)
T (Tao) is a constant like alpha. The higher the value of Tao, the higher is the range of the input points used (chosen) for prediction or wider is the weighting function and vice-versa.

to the term "(alpha/number_inputs)*(Yi-h_theta)*(Xi[i]))" in the map function of the code.

This algorithm takes in 5 arguments as follows:

1. The number of features each input point has
2. The value of alpha
3. The number of times you want your algorithm to iterate
4. The input path
5. The output path