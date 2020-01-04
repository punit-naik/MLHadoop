package lud.naiveGaussian.mergeResults;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lud.io.TextPair;

public class merge_results_mapper extends Mapper<LongWritable, Text, TextPair, Text> {
	
	private Boolean upper;
	private int total_records;
	
	@Override
	public void setup (Context context) {
		this.upper = context.getConfiguration().getBoolean("upper", false);
		this.total_records = (int) context.getConfiguration().getLong("total_records", 0);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().split("\\t");
		// Processing Upper Triangular Matrix's rows
		if (this.upper && !parts[0].contains(",")) {
			context.write(new TextPair(parts[0],""), new Text(parts[1]));
		}
		// Processing Lower Triangular Matrix's rows
		if (!this.upper && parts[0].contains(",")) {
			
			String[] rowCol = parts[0].split(",");
			String row = rowCol[0];
			// Sending first row of Lower Triangular Matrix to the reducer
			if (Integer.valueOf(row)-1 == 0) {
				for (int i = 0; i < this.total_records; i++) {
					context.write(new TextPair("0",String.valueOf(i)), new Text(i+","+((i == 0) ? 1 : 0)));
				}
			}
			String column = rowCol[1];
			String element = parts[1];
			context.write(new TextPair(row, column), new Text(column+","+element));
		}
	}

}
