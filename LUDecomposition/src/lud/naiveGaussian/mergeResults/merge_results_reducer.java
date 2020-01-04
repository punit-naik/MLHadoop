package lud.naiveGaussian.mergeResults;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import lud.Utils;
import lud.io.TextPair;

public class merge_results_reducer extends Reducer<TextPair, Text, TextPair, Text> {
	
	private Boolean upper;
	private int total_records;
	
	@Override
	public void setup (Context context) {
		this.upper = context.getConfiguration().getBoolean("upper", false);
		this.total_records = (int) context.getConfiguration().getLong("total_records", 0);
	}
	
	public static String arrayToCSV(String[] a) {
        String result = "";
        if (a.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String s : a) {
                sb.append(s).append(",");
            }
            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }
	
	public void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (this.upper) {
			for (Text val:values) {
				context.write(new TextPair(key.getFirst(),""), val);
			}
		}
		else {
			Double[] rowElements = new Double[this.total_records];
			int row = Integer.valueOf(key.getFirst());
			for (Text val:values) {
				String[] parts = val.toString().split(",");
				int j = Integer.valueOf(parts[0]);
				rowElements[j] = Double.valueOf(parts[1]);
			}
			// Setting Diagonal Elements as `1` in the lower triangular matrix rows
			rowElements[row] = (double) 1;
			
			for(int j = 0; j< this.total_records; j++) {
				if (rowElements[j] == null) {
					rowElements[j] = (double) 0;
				}
			}
			context.write(new TextPair(key.getFirst(),""), new Text(Utils.arrayToCSV(rowElements)));
		}
	}

}
