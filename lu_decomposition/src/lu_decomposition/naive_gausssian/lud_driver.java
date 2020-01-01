package lu_decomposition.naive_gausssian;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lu_decomposition.naive_gausssian.FindNthRow.find_nth_driver;
import lu_decomposition.naive_gausssian.MergeResults.merge_results_driver;
import lu_decomposition.naive_gausssian.TotalRecords.total_records_driver;

public class lud_driver {
	
	public static String arrayToCSV(String[] a) {
        String result = "";
        if (a.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String s : a) {
                sb.append(s).append(",");
            }
            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }

	@SuppressWarnings("deprecation")
	public static boolean runWithConf (String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
	  
	  Job job = new Job(conf);
	
	  job.setJarByClass(lud_driver.class);
	
	  job.setJobName("Split a matrix into it's LU decomposed components using the Naive Gaussian Elimination method");
	  long n = conf.getLong("n", 0);
	  FileInputFormat.setInputPaths(job, new Path((n==0)?args[0]:(args[1]+"-run-"+(n-1))));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]+"-run-"+n));
	  job.setNumReduceTasks(0);
	  job.setMapperClass(lud_mapper.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	
	  boolean success = job.waitForCompletion(true);
	  
	  return success;
	};
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String input = args[0];
		String output = args[1];
		String total_records_output = output + "/total_records";
		String[] total_records_args = {input, total_records_output};
		String find_nth_row_output = output + "/nth";
		// MR Job: Finding Total Records
		long total_records = total_records_driver.run(total_records_args);
		String[] lud_args = {input, output};
		Configuration conf = new Configuration();
		
		for(long n = 0; n < total_records-1; n++) {
			String find_nth_row_input = (n==0) ? input : output+"-run-"+(n-1);
			String[] find_nth_row_args = {find_nth_row_input, find_nth_row_output};
			// MR Job: Finding Nth Record
			String nVal = find_nth_driver.run(find_nth_row_args, n, total_records);
			conf.setLong("n", n);
		    conf.setLong("total_records", total_records);
			conf.set("nVal", nVal);
			// MR Job: Running LU Decomposition on the input
			runWithConf(lud_args, conf);
		}
		
		// MR Job(s): Merging Outputs
		conf.setBoolean("upper", false);
		Job job = new Job(conf);
		String[] path = new String[(int) (total_records-1)];
		for(long n = 0; n < total_records-1; n++) {
			path[(int) n] = (output+"-run-"+n);
		}
		FileInputFormat.setInputPaths(job, arrayToCSV(path));
		merge_results_driver.runWithJob(job, output+"-merged/lower");	
		conf.setBoolean("upper", true);
		job = new Job(conf);
		FileInputFormat.addInputPath(job, new Path(output+"-run-"+(total_records-2)));
		merge_results_driver.runWithJob(job, output+"-merged/upper");
	}
}
