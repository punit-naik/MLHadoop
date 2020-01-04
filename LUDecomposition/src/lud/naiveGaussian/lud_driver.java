package lud.naiveGaussian;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lud.naiveGaussian.mergeResults.merge_results_driver;
import lud.naiveGaussian.totalRecords.total_records_driver;

public class lud_driver {
	
	public static void prepareJobWithConf (Job jobPrep, Configuration confPrep) throws IOException, InterruptedException, ClassNotFoundException {
		
		long n = confPrep.getLong("n", 0);
		
		// Chaining MR Jobs
		if (n == 0) {
			ChainMapper.addMapper(jobPrep, initial_input_mapper.class,
					Text.class, Text.class,
					Text.class, Text.class,
					confPrep);
			ChainReducer.setReducer(jobPrep, lud_reducer.class, Text.class, Text.class, Text.class, Text.class, confPrep);
		}
		else {
			ChainReducer.addMapper(jobPrep, initial_input_mapper.class,
					Text.class, Text.class,
					Text.class, Text.class,
					confPrep);
			ChainReducer.addMapper(jobPrep, lud_mapper.class,
					Text.class, Text.class,
					Text.class, Text.class,
					confPrep);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = args[0];
		String output = args[1];
		String find_nth_row_output = output + "/nth";
		
		// MR Job: Finding Total Records
		
		String[] total_records_args = {input, output + "/total_records"};
		long total_records = total_records_driver.run(total_records_args);
		
		Configuration conf = new Configuration();
		conf.set("find_nth_row_output", find_nth_row_output);
		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1.00");
		conf.setLong("total_records", total_records);
		Job job = new Job(conf);
		
		for(int n = 0 ; n < total_records - 1 ; n++) {
			
			Configuration confLoop = conf;
			confLoop.set("mapreduce.job.reduce.slowstart.completedmaps", "1.00");
			confLoop.unset("n");
			confLoop.setLong("n", n);
			
			prepareJobWithConf(job, confLoop);
		}
		
		String lud_output_path = output+"/after-"+(total_records-1)+"-runs";
		job.setJarByClass(lud_driver.class);
		job.setJobName("Split a matrix into it's LU decomposed components using the Naive Gaussian Elimination method");
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(lud_output_path));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		
		// MR Job(s): Merging Outputs
		
		Path merge_results_input_path = new Path(lud_output_path);
		conf.setBoolean("upper", false);
		job = new Job(conf);
		FileInputFormat.addInputPath(job, merge_results_input_path);
		merge_results_driver.runWithJob(job, output+"/LU_Components/L");
		
		conf.setBoolean("upper", true);
		job = new Job(conf);
		FileInputFormat.addInputPath(job, merge_results_input_path);
		merge_results_driver.runWithJob(job, output+"/LU_Components/U");
	}
}
