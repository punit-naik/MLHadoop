import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		int num_features=0;
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		//args[0] is the path to the file which has features of the input waiting to be classified.
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[0]))));
		String line=null;
		while((line=br.readLine())!=null){
			String[] feat=line.toString().split("\\ ");
			for(int i=0;i<feat.length;i++)
				conf.setFloat("feat"+i, Float.parseFloat(feat[i]));
			num_features=feat.length;
			break;
		}
		br.close();
		hdfs.close();
		conf.setInt("num_features",num_features);
		//args[1] is the name of the entity to be classified.
		conf.set("name",args[1]);
		Job job = new Job(conf,"KNN Classification MapReduce");
		job.setJarByClass(Driver.class);
		//args[2] is the path to the input file which will be used for training.
		FileInputFormat.setInputPaths(job, new Path(args[2]));
		//args[3] is the path to the output file.
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
