package lud.naiveGaussian.mergeResults;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lud.io.NaturalKeyGroupingComparator;
import lud.io.TextPair;
import lud.io.TextPairComparator;
import lud.io.TextPairPartitioner;

public class merge_results_driver {

	public static boolean runWithJob(Job job, String out_path) throws IOException, InterruptedException, ClassNotFoundException {
	  job.setJarByClass(merge_results_driver.class);
	
	  job.setJobName("Final Step: Merging results and creating separate LU decomposed components of input matrix");
	
	  FileOutputFormat.setOutputPath(job, new Path(out_path));
	
	  job.setMapperClass(lud.naiveGaussian.mergeResults.merge_results_mapper.class);
	  job.setReducerClass(lud.naiveGaussian.mergeResults.merge_results_reducer.class);
	  job.setMapOutputKeyClass(TextPair.class);
	  job.setMapOutputValueClass(Text.class);
	  job.setOutputKeyClass(TextPair.class);
	  job.setOutputValueClass(Text.class);
	  job.setPartitionerClass(TextPairPartitioner.class);
      job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
      job.setSortComparatorClass(TextPairComparator.class);
      
      boolean success = job.waitForCompletion(true);
	  return success;
	};
}
