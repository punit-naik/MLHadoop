import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MF_Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(MF_Driver.class);
		job.setJobName("Mutual Friend Calculator");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MF_Mapper.class);
		job.setCombinerClass(MF_Reducer.class);
		job.setReducerClass(MF_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	};

}
