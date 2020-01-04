package lud.naiveGaussian;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lud.Utils;

public class lud_mapper extends Mapper<Text, Text, Text, Text> {
	
	private static long total_records;
	private long n;
	private Double[] nVal = null;
	
	public void setup (Context context) throws IOException, InterruptedException {
		lud_mapper.total_records = context.getConfiguration().getLong("total_records", 0);
		this.n = context.getConfiguration().getLong("n", 0);
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] parts = new String[2];
		parts[0] = key.toString();
		String[] rowAndNVal = value.toString().split(";");
		parts[1] = parts[0].contains(",")?value.toString():rowAndNVal[0];
		
		if (this.nVal == null && !parts[0].contains(","))
			this.nVal = Utils.stringToDoubleArray(rowAndNVal[1].split(","));
		
		if(!parts[0].contains(",")) {
			
			long row = Long.valueOf(parts[0]);
			
			if (row > this.n) {
				
				Double[] rowElements = Utils.stringToDoubleArray(parts[1].split(","));
				Double multiplier = (double) (rowElements[(int) this.n]/this.nVal[(int) this.n]);
				// Sending lower triangular matrix elements
				context.write(new Text(row+","+this.n), new Text(String.valueOf(multiplier)));
				Double[] rowElementsModified = new Double[(int) lud_mapper.total_records];
				
				for (int i = 0; i< lud_mapper.total_records; i++) {
					rowElementsModified[i] = (Double) (rowElements[i] - this.nVal[i]*multiplier);
				}
				
				context.write(new Text(String.valueOf(row)), new Text(Utils.arrayToCSV(rowElementsModified)));
			}
			else
			  context.write(new Text(parts[0]), new Text(parts[1].split(";")[0]));
		}
		else
		  context.write(new Text(parts[0]), new Text(parts[1].split(";")[0]));
	}
}
