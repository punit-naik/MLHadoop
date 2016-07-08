import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansCentroidCalculationDriver_ClassificationDriver{
	public static boolean isdone=false;
	public static String num_centers;
	public static String dimension;
	public static void main(String[] args) throws Exception{
		Configuration conf= new Configuration();
		//args[0] is the number of centers to be used.
		num_centers=args[0];
		//args[1] is the dimension of the input.
		dimension=args[1];
		conf.setInt("noc", Integer.parseInt(num_centers));
		conf.setInt("dimension", Integer.parseInt(dimension));
		int iter=0;
		FileSystem hdfs=FileSystem.get(conf);
		ArrayList<Float> centers=new ArrayList<Float>();
		//args[3] is the output path. Initially it will contain a single file
		//in which old and new centroids will be assigned to 0.0.
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[3]))));
		String line=null;
		while((line=br.readLine())!=null){
			++iter;
			String[] tok=line.split("\\\t");
			String[] centroids_new= tok[1].split("\\,");
			centers.add(Float.parseFloat(centroids_new[0]));
			centers.add(Float.parseFloat(centroids_new[1]));
		}
		br.close();
		for(int i=1;i<=Integer.parseInt(num_centers);i++){
			if(iter==i){
				for(int j=0;j<((Integer.parseInt(num_centers)-iter)*2);j++){
					centers.add((float) 0);
				}
			}
		}
		if(hdfs.exists(new Path(args[3]))){
			hdfs.delete(new Path(args[3]),true);
		}
		hdfs.close();
		for(int i=0;i<(Integer.parseInt(num_centers)*2);i++){
			conf.setFloat("c".concat(String.valueOf(i)) , centers.get(i));
		}
		Job job = new Job(conf,"K-Means Clustering MapReduce");
		job.setJarByClass(KMeansCentroidCalculationDriver_ClassificationDriver.class);
		//args[2] is the input path.
		FileInputFormat.setInputPaths(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(KMeansCentroidCalculationMap.class);
		job.setCombinerClass(KMeansCentroidCalculationReduce.class);
		job.setReducerClass(KMeansCentroidCalculationReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		while(isdone==false){
			run(args);
		}
	}
	public static void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf_med=new Configuration();
		conf_med.setInt("noc", Integer.parseInt(num_centers));
		conf_med.setInt("dimension", Integer.parseInt(dimension));
		int iter_med=0;
		ArrayList<Float> centers_old=new ArrayList<Float>();
		ArrayList<Float> centers_new=new ArrayList<Float>(); 
		FileSystem hdfs_med=FileSystem.get(conf_med);
		BufferedReader br_med = new BufferedReader(new InputStreamReader(hdfs_med.open(new Path(args[3]))));
		String line_med=null;
		while((line_med=br_med.readLine())!=null){
			++iter_med;
			String[] tok= line_med.split("\\\t");
			String[] centroids_old= tok[0].split("\\,");
			String[] centroids_new= tok[1].split("\\,");
			centers_old.add(Float.parseFloat(centroids_old[0]));
			centers_old.add(Float.parseFloat(centroids_old[1]));
			centers_new.add(Float.parseFloat(centroids_new[0]));
			centers_new.add(Float.parseFloat(centroids_new[1]));
		}
		br_med.close();
		for(int i=1;i<=Integer.parseInt(num_centers);i++){
			if(iter_med==i){
				for(int j=0;j<((Integer.parseInt(num_centers)-iter_med)*2);j++){
					centers_old.add((float) 0);
					centers_new.add((float) 0);
				}
			}
		}
		if(hdfs_med.exists(new Path(args[3]))){
			hdfs_med.delete(new Path(args[3]),true);
		}
		hdfs_med.close();
		ArrayList<Float> ond = new ArrayList<Float>();
		for(int i=0;i<Integer.parseInt(num_centers)*2;i+=2){
			ond.add(Math.abs((centers_old.get(i)-centers_new.get(i))+(centers_old.get(i+1)-centers_new.get(i+1))));
		}
		int check=0;
		for(int i=0;i<Integer.parseInt(num_centers);i++){
			if(ond.get(i)<=0.0002){
				check=1;
			}
			else{
				check=0;
				break;
			}
		}

		if(check==1){
			isdone=true;
			Configuration conf_new= new Configuration();
			conf_new.setInt("noc", Integer.parseInt(num_centers));
			conf_new.setInt("dimension", Integer.parseInt(dimension));
			for(int i=0;i<(Integer.parseInt(num_centers)*2);i++){
				conf_new.setFloat("c".concat(String.valueOf(i)), centers_new.get(i));
			}
			Job job_new = new Job(conf_new,"K-Means Clustering MapReduce");
			job_new.setJarByClass(KMeansCentroidCalculationDriver_ClassificationDriver.class);
			FileInputFormat.setInputPaths(job_new, new Path(args[2]));
			FileOutputFormat.setOutputPath(job_new, new Path(args[3]));
			job_new.setMapperClass(KMeansCentroidCalculationMap.class);
			job_new.setCombinerClass(KMeansClassificationReduce.class);
			job_new.setReducerClass(KMeansClassificationReduce.class);
			job_new.setMapOutputKeyClass(Text.class);
			job_new.setMapOutputValueClass(Text.class);
			job_new.setOutputKeyClass(Text.class);
			job_new.setOutputValueClass(Text.class);
			System.exit(job_new.waitForCompletion(true)?0:1);
		}
		else{
			isdone=false;
			for(int i=0;i<(Integer.parseInt(num_centers)*2);i++){
				conf_med.setFloat("c".concat(String.valueOf(i)) , centers_new.get(i));
			}
			Job job_med = new Job(conf_med,"K-Means Clustering MapReduce");
			job_med.setJarByClass(KMeansCentroidCalculationDriver_ClassificationDriver.class);
			FileInputFormat.setInputPaths(job_med, new Path(args[2]));
			FileOutputFormat.setOutputPath(job_med, new Path(args[3]));
			job_med.setMapperClass(KMeansCentroidCalculationMap.class);
			job_med.setCombinerClass(KMeansCentroidCalculationReduce.class);
			job_med.setReducerClass(KMeansCentroidCalculationReduce.class);
			job_med.setMapOutputKeyClass(Text.class);
			job_med.setMapOutputValueClass(Text.class);
			job_med.setOutputKeyClass(Text.class);
			job_med.setOutputValueClass(Text.class);
			job_med.waitForCompletion(true);
		}
	}
}
