import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MBA_Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		conf.set("group_num", args[2]);
		Job job = new Job(conf);
		job.setJarByClass(MBA_Driver.class);
		job.setJobName("Market Basket Analysis");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MBA_Mapper.class);
		job.setCombinerClass(MBA_Reducer.class);
		job.setReducerClass(MBA_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
