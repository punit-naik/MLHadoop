package lu_decomposition.naive_gausssian.FindNthRow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lu_decomposition.naive_gausssian.io.LongAndTextWritable;

public class find_nth_mapper extends Mapper<LongWritable, Text, NullWritable, LongAndTextWritable> {
	
	private LongWritable nthKey;
	private Text nthValue = null;
	
	@Override
	public void setup (Context context) throws IOException, InterruptedException {
		this.nthKey = new LongWritable(context.getConfiguration().getLong("n", 0));
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] input = value.toString().split("\\t");
		if (!input[0].contains(",")) {
			LongWritable rowKey = new LongWritable(Long.valueOf(input[0]));
			if (rowKey.compareTo(this.nthKey) == 0) {
				this.nthValue = new Text(input[1]);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		if (this.nthValue != null)
			context.write(null, new LongAndTextWritable(this.nthKey, this.nthValue));
	}

}
