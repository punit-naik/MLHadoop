import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FinalMap extends Mapper<LongWritable, Text, Text, Text> {
	public static String delimiter=null;
	public static HashMap<String,String> map=new HashMap<String,String>();
	@Override
	public void setup(Context context){
		delimiter=context.getConfiguration().get("delimiter");
	}
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] parts=value.toString().split("\\;");
		String score=parts[1];
		String[] parts2=parts[0].split(delimiter);
		String[] parts3=parts2[1].split("\\,");
		String user=parts3[1];
		String item=parts3[0];
		if(!map.containsKey(user)){
			map.put(user, item+","+score);
		}
		else{
			String[] old=map.get(user).split(",");
			if(Double.parseDouble(score)>Double.parseDouble(old[0])){
				map.put(user, item+","+score);
			}
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(Entry<String,String> entry:map.entrySet()){
			context.write(new Text(entry.getKey()), new Text(entry.getValue()));
		}
	}
}
