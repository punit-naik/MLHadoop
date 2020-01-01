package lu_decomposition.naive_gausssian.FindNthRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lu_decomposition.naive_gausssian.io.LongAndTextWritable;

public class find_nth_driver {
	
	public static String readNthRow (String path, Configuration conf) throws IOException {
		FileSystem hdfs=FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(path+"/part-m-00000"))));
		String records = br.readLine().split("\\s")[1];
		br.close();
		return records;
	}
	
	public static String run (String[] args, long n, long total_records) throws IOException, InterruptedException, ClassNotFoundException {
		return (n <= total_records-1) ? runSafely(args, n) :"fail";
	}

	@SuppressWarnings("deprecation")
	public static String runSafely (String[] args, long n) throws IOException, InterruptedException, ClassNotFoundException {
	  Configuration conf= new Configuration();
	  FileSystem hdfs=FileSystem.get(conf);
	  // Deleting previous stored nth row
	  hdfs.delete(new Path(args[1]));
	  conf.setLong("n", n);
	  Job job = new Job(conf);
	
	  job.setJarByClass(find_nth_driver.class);
	
	  job.setJobName("Finds the nth row of the HDFS file");
	
	  FileInputFormat.setInputPaths(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	  job.setMapperClass(find_nth_mapper.class);
	  job.setNumReduceTasks(0);
	  job.setOutputKeyClass(NullWritable.class);
	  job.setOutputValueClass(LongAndTextWritable.class);
	
	  job.waitForCompletion(true);
	  
	  return readNthRow(args[1], conf);
	};

}
