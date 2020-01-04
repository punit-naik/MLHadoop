package lud.naiveGaussian.totalRecords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class total_records_mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	private Long countRows = (long) 0;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		this.countRows++;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		context.write(new LongWritable(0), new LongWritable(this.countRows));
	}

}
