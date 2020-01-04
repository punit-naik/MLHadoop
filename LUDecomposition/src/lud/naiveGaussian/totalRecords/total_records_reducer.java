package lud.naiveGaussian.totalRecords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class total_records_reducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	private Long countRows = (long) 0;
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		for(LongWritable val:values){
			this.countRows += val.get();
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		context.write(new LongWritable(0), new LongWritable(this.countRows));
	}

}
