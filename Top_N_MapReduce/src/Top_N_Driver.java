import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top_N_Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		conf.set("N", args[0]);
		Job job = new Job(conf);
		job.setJarByClass(Top_N_Driver.class);
		job.setJobName("Top_N_Driver");
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Top_N_Mapper.class);
		job.setReducerClass(Top_N_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	};

}
