import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBCReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Double out_value=Double.NEGATIVE_INFINITY;
		String out_key=null;
		HashMap<String,Integer> final_output=new HashMap<String,Integer>();
		for(Text value:values){
			if(final_output.containsKey(value.toString()))
				final_output.put(value.toString(), final_output.get(value.toString())+1);
			else
				final_output.put(value.toString(), 1);
		}
		for(Entry<String,Integer> output:final_output.entrySet()){
			if(output.getValue()>out_value){
				out_value=(double) output.getValue();
				out_key=output.getKey();
			}
		}
		context.write(null, new Text(out_key));
	}
}
