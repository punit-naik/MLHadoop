package lud.naiveGaussian;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lud.Utils;
import lud.io.TextPair;

public class initial_input_mapper extends Mapper<Text, Text, Text, Text> {
	
	private int n;
	private String nVal = null;
	
	private long counter = 0;
	private long[] input_range = new long[2];
	
	private List<TextPair> toBeSent = new ArrayList<TextPair>();
	
	public static Double[] readNthRow (Configuration conf) throws IOException {
		try {
			
			String path = conf.get("find_nth_row_output");
			String[] nValArr = Utils.readFromHDFS(path, conf).split(",");
			return Utils.stringToDoubleArray(nValArr);
			
		}
		catch (Exception e) {
			System.out.println("Can't read nth value! " + e.getMessage());
			return null;
		}
	}
	
	@Override
	public void setup (Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.n = (int) conf.getLong("n", 0);
		// The below code should have worked, but it does not
		//if (n>0)
		//	this.nVal = Utils.arrayToCSV(readNthRow(context.getConfiguration()));
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		if (counter == 0 && !key.toString().contains(","))
			this.input_range[0] = Long.valueOf(key.toString());

		if (!key.toString().contains(",")) {
			int row = Integer.parseInt(key.toString());
			
			if (row == this.n)
				this.nVal = value.toString();
			
			if (n > 0)
				//context.write(key, new Text(value.toString()+";"+this.nVal));
				toBeSent.add(new TextPair(key.toString(), value.toString()));
			else
				context.write(key, value);
		}
		else
			context.write(key, value);
		
		counter++;
	}

	@Override
	public void cleanup (Context context) throws IOException, InterruptedException {
		
		// The code block below will run if the nth row is not in the split.
		// It will read it from HDFS, which was stored there from the previous mapper/reducer
		if (this.nVal == null && n>0)
		  this.nVal = Utils.arrayToCSV(readNthRow(context.getConfiguration()));
		
		if (n == 0 && n>=input_range[0] && n<=input_range[1]) {
			input_range[1] = input_range[0] + counter - 1;
			// Sending Nth Row to all reducers
			for (long i = 0 ; i <= input_range[1] ; i++)
				context.write(new Text(String.valueOf(i)), new Text("Nth Row->"+this.nVal));
		}
		
		// Have to do this because there is a mapper after this map phase, and not a reducer.
		// Otherwise I would have used the same logic like in the if block above.
		else
			for (TextPair tp:toBeSent)
				context.write(new Text(tp.getFirst()), new Text(tp.getSecond()+";"+this.nVal));
	}
}
