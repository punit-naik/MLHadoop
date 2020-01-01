package lu_decomposition.naive_gausssian;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class lud_mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private int n;
	private Double[] nVal;
	private int total_records;
	
	public Double[] stringToDoubleArray(String[] a) {
		
		Double[] x = new Double[a.length];
		
		for(int i = 0; i < this.total_records; i++) {
			x[i] = Double.valueOf(a[i]);
		}
		
		return x;
		
	}
	
	public static String arrayToCSV(Double[] nVal2) {
        String result = "";

        if (nVal2.length > 0) {
            StringBuilder sb = new StringBuilder();

            for (Double s : nVal2) {
                sb.append(s).append(",");
            }

            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }
	
	@Override
	public void setup (Context context) throws IOException, InterruptedException {
		this.n = (int) context.getConfiguration().getLong("n", 0);
		this.total_records = (int) context.getConfiguration().getLong("total_records", 0);
		this.nVal =  stringToDoubleArray(context.getConfiguration().get("nVal").split(","));
		
		context.write(new Text(String.valueOf(this.n)), new Text(arrayToCSV(this.nVal)));
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().split("\\t");
		if(!parts[0].contains(",")) {
			long row = Long.valueOf(parts[0]);
			if (row > this.n) {
				Double[] rowElements = stringToDoubleArray(parts[1].split(","));
				Double multiplier = (double) (rowElements[this.n]/this.nVal[this.n]);
				context.write(new Text(row+","+this.n), new Text(String.valueOf(multiplier)));
				Double[] rowElementsModified = new Double[this.total_records];
				for (int i = 0; i< this.total_records; i++) {
					rowElementsModified[i] = (Double) (rowElements[i] - this.nVal[i]*multiplier);
				}
				if (row != 0)
					context.write(new Text(String.valueOf(row)), new Text(arrayToCSV(rowElementsModified)));
			}
			else {
				if (Long.valueOf(parts[0]) != this.n)
					context.write(new Text(parts[0]), new Text(parts[1]));
			}
		}
	}

}
