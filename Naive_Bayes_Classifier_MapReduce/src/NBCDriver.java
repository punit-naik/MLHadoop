import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NBCDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		// The test input for which you want to find the acitivity that the Person should be doing
		conf.set("test_input", args[0]);
		Job job = new Job(conf);
		job.setJarByClass(NBCDriver.class);
		job.setJobName("Naive_Bayes_calssifier using Hadoop");
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(NBCMap.class);
		job.setReducerClass(NBCReduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
