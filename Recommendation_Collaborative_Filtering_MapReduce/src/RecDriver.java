import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RecDriver {
	public static String delimiter="-->";
	public static String outFile=null;
	public static String rec_in=null;
	public static String mid_out=null;
	public static String final_out=null;
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// args[0] is the path of the file which stores the number of unique items "n" and its identification
		// which is the task_id.
		// It also the "n" part of matrices co_oc_mat and user_scoring_mat
		// where co_oc_mat has dimensions of m x n
		// and sorted_user_scoring_mat has dimensions n x p
		String a=String.valueOf(args[0].charAt(args[0].length()-1));
		if(!"/".contentEquals(a)){
			args[0]=args[0]+"/";
		}
		outFile=args[0]+"n.txt";
		
		//args[1] is the input file.
		rec_in=args[1];
		
		//args[2] is the intermediate output which is also the input to final recommendation job.
		mid_out=args[2];
		
		//args[3] is the final output.
		final_out=args[3];
		
		run1(args);
		run2(args);
	}
	public static void run1(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("outFile", outFile);
		conf.set("delimiter", delimiter);
		Job job = new Job(conf, "Recommendations_CollaborativeFiltering_Prepare");
		job.setJarByClass(RecDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(RecMap.class);
		job.setReducerClass(RecReduce.class);
		FileInputFormat.addInputPath(job, new Path(rec_in));
		FileOutputFormat.setOutputPath(job, new Path(mid_out));
		job.waitForCompletion(true);
	}
	public static void run2(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("delimiter", delimiter);
		Job job = new Job(conf, "Recommendations_CollaborativeFiltering_Final");
		job.setJarByClass(RecDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FinalMap.class);
		job.setReducerClass(FinalReduce.class);
		FileInputFormat.addInputPath(job, new Path(mid_out));
		FileOutputFormat.setOutputPath(job, new Path(final_out));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
