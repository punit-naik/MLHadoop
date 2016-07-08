import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatMulDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		// A is an m-by-n matrix; B is an n-by-p matrix.
		conf.set("m", args[0]);
		conf.set("n", args[1]);
		conf.set("p", args[2]);
		Job job = new Job(conf, "Matrix_Multiplication");
		job.setJarByClass(MatMulDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MatMulMap.class);
		//Don't use combiner if there is no scope of combining the output. Otherwise the job will get stuck.
		//job.setCombinerClass(MatMulModGenReduce.class);
		job.setReducerClass(MatMulReduce.class);
		//args[3] is the input path.
		FileInputFormat.addInputPath(job, new Path(args[3]));
		//args[4] is the output path.
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}