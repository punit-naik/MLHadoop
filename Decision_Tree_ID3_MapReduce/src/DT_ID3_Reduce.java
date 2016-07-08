import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DT_ID3_Reduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		HashMap<String,Integer> counts=new HashMap<String,Integer>();
		String maxKey="";
		int maxValue=-1;
		for(Text value:values){
			if(counts.containsKey(value.toString()))
				counts.put(value.toString(), counts.get(value.toString())+1);
			else
				counts.put(value.toString(), 1);
		}
		for(Entry<String,Integer> e:counts.entrySet()){
			if(e.getValue()>maxValue){
				maxKey=e.getKey();
				maxValue=e.getValue();
			}
		}
		context.write(null, new Text(maxKey));
	}
}
