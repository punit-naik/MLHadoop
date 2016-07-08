import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static int num_features; // needs to be set
	public static float alpha; // needs to be set
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		//args[0] is the number of features each input has.
		num_features=Integer.parseInt(args[0]);
		++num_features;
		//args[1] is the value of alpha that you want to use.
		alpha=Float.parseFloat(args[1]);
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		Float[] theta=new Float[num_features];
		//args[2] is the number of times you want to iterate over your training set.
		for(int i=0;i<Integer.parseInt(args[2]);i++){
			//for the first run
			if(i==0){
				for(int i1=0;i1<num_features;i1++){
					theta[i1]=(float) 0;
				}
			}
			//for the second run
			else{
				int iter=0;
				//args[4] is the output path for storing the theta values.
				BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[4]))));
				String line1=null;
				while((line1=br1.readLine())!=null){
					String[] theta_line=line1.toString().split("\t");
					theta[iter]=Float.parseFloat(theta_line[1]);
					iter++;
				}
				br1.close();
			}
			if(hdfs.exists(new Path(args[4]))){
				hdfs.delete(new Path(args[4]),true);
			}
			hdfs.close();
			//alpha value initialisation
			conf.setFloat("alpha", alpha);
			//Theta Value Initialisation
			for(int j=0;j<num_features;j++){
				conf.setFloat("theta".concat(String.valueOf(j)), theta[j]);
			}
			Job job = new Job(conf,"Calculation of Theta");
			job.setJarByClass(Driver.class);
			//args[3] is the input path.
			FileInputFormat.setInputPaths(job, new Path(args[3]));
			FileOutputFormat.setOutputPath(job, new Path(args[4]));
			job.setMapperClass(thetaMAP.class);
			job.setReducerClass(thetaREDUCE.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			job.waitForCompletion(true);
		}
	}  
}
