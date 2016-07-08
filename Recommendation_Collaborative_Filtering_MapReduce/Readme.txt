This is an Algorithm which generates Recommendations for users by using the Collaborative Filtering technique.

This algorithm takes in four arguments, namely:

1. args[0]: The path which will store the value "n" for a particular task_id. It also the "n" part of matrices co_oc_mat and user_scoring_mat where co_oc_mat has dimensions of m x n and sorted_user_scoring_mat has dimensions n x p.

2. args[1]: The path to the input.

3. args[2]: The intermediate output of the program which is also the input to the final MR Job.

4. args[3]: The final output path which will contain recommendations for users. Each group of users will be identified by their task_IDs.