package lu_decomposition.naive_gausssian.TotalRecords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class total_records_driver {
	
	public static long readTotalRecords (String path, Configuration conf) throws IOException {
		FileSystem hdfs=FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(path+"/part-r-00000"))));
		Long records = (long) 0;
		records = Long.valueOf(br.readLine().split("\\t")[1]);
		br.close();
		return records;
	}

	@SuppressWarnings("deprecation")
	public static long run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	  Configuration conf = new Configuration();
	  Job job = new Job(conf);
	
	  job.setJarByClass(total_records_driver.class);
	
	  job.setJobName("Just counting total rows of the HDFS input");
	
	  FileInputFormat.setInputPaths(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	  job.setMapperClass(total_records_mapper.class);
	
	  job.setReducerClass(total_records_reducer.class);
	  job.setCombinerClass(total_records_reducer.class);
	
	  job.setOutputKeyClass(LongWritable.class);
	  job.setOutputValueClass(LongWritable.class);
	  
	  //job.setInputFormatClass(TextInputFormat.class);
      //job.setOutputFormatClass(TextOutputFormat.class);
	
	  job.waitForCompletion(true);
	  
	  return readTotalRecords(args[1], conf);
  };
}
