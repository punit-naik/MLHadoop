package lud.naiveGaussian;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import lud.Utils;

public class lud_reducer extends Reducer<Text, Text, Text, Text> {
	
	private static long total_records;
	private long n;
	private Double[] nVal = null;
	
	public void setup (Context context) throws IOException, InterruptedException {
		lud_reducer.total_records = context.getConfiguration().getLong("total_records", 0);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// Fetching Nth Row from values
		for (Text value:values) {
			
			String[] parts = new String[2];
			parts[0] = key.toString();
			parts[1] = value.toString();
			
			if (parts[1].contains("Nth Row->")) {
				this.nVal = Utils.stringToDoubleArray(parts[1].split("->")[1].split(","));
				break;
			}
		}
		
		// Processing rest of the rows
		for (Text value:values) {
			
			String[] parts = new String[2];
			parts[0] = key.toString();
			parts[1] = value.toString();
			
			if (parts[1].contains("Nth Row->"))
				continue;
			
			else {
				if(!parts[0].contains(",")) {
					
					long row = Long.valueOf(parts[0]);
					
					if (row > this.n) {
						Double[] rowElements = Utils.stringToDoubleArray(parts[1].split(","));
						Double multiplier = (double) (rowElements[(int) this.n]/this.nVal[(int) this.n]);
						
						context.write(new Text(row+","+this.n), new Text(String.valueOf(multiplier)));
						
						Double[] rowElementsModified = new Double[(int) lud_reducer.total_records];
						for (int i = 0; i< lud_reducer.total_records; i++) {
							rowElementsModified[i] = (Double) (rowElements[i] - this.nVal[i]*multiplier);
						}
						
						context.write(new Text(String.valueOf(row)), new Text(Utils.arrayToCSV(rowElementsModified)));
					}
					else
					  context.write(new Text(parts[0]), new Text(parts[1]));
				}
				else
				  context.write(new Text(parts[0]), new Text(parts[1]));
			}
		}
	}
}
