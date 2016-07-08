import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NBCMap extends Mapper<LongWritable, Text, IntWritable, Text>{
	public static String output_key;
	public static String[] test_input=null;
	public static int count=0;
	public static HashMap<String,Integer> inputs=new HashMap<String,Integer>();
	public static double output_value=Double.NEGATIVE_INFINITY;
	public static HashMap<String,Double> output= new HashMap<String,Double>();
	public static HashMap<String,Double> outcome_count= new HashMap<String,Double>();
	public static HashMap<String,Double> features_count= new HashMap<String,Double>();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(test_input==null)
			test_input=context.getConfiguration().get("test_input").split("\\,");
		String[] input=value.toString().split("\\,");
		for(int j=0;j<input.length;j++){
			if(j==input.length-1){
				if(outcome_count.containsKey(input[j]))
					outcome_count.put(input[j], outcome_count.get(input[j])+1);
				else
					outcome_count.put(input[j], (double) 1);
			}
			else{
				if(input[j].contentEquals(test_input[j])){
					if(!inputs.containsKey(j+","+input[j]))
						inputs.put(j+","+input[j], 0);
					if(features_count.containsKey(j+","+input[j]+"|"+input[input.length-1]))
						features_count.put(j+","+input[j]+"|"+input[input.length-1], features_count.get(j+","+input[j]+"|"+input[input.length-1])+1);
					else
						features_count.put(j+","+input[j]+"|"+input[input.length-1], (double) 1);
				}
			}
		}
		++count;
	}
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		for(Entry<String,Double> o_c:outcome_count.entrySet()){
			String output_class=o_c.getKey();
			for(Entry<String,Integer> i:inputs.entrySet()){
				if(!features_count.containsKey(i.getKey()+"|"+output_class))
					features_count.put(i.getKey()+"|"+output_class, (double) 0);
			}
			double output_class_count=o_c.getValue();
			double probability=output_class_count/count;
			for(Entry<String,Double> f_c:features_count.entrySet()){
				if(f_c.getKey().split("\\|")[1].contentEquals(output_class))
					probability=probability*(f_c.getValue()/output_class_count);
			}
			output.put(output_class, probability);
		}
		for(Entry<String,Double> o:output.entrySet()){
			if(o.getValue()>output_value){
				output_value=o.getValue();
				output_key=o.getKey();
			}
		}
		context.write(new IntWritable(1),new Text(output_key));
	}
}
