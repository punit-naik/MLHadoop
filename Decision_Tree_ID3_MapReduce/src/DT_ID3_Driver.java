import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DT_ID3_Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf=new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(DT_ID3_Driver.class);
		job.setJobName("Decision_Tree_Algorithm_on_Hadoop");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setNumReduceTasks(0);
		job.setMapperClass(DT_ID3_Map.class);
		job.setReducerClass(DT_ID3_Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
