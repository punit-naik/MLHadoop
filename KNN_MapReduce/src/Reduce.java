import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	String flower_name=null;
	@Override
	public void setup(Context context){
		flower_name=String.valueOf(context.getConfiguration().get("name"));
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		HashMap<String,Integer> map=new HashMap<String,Integer>();
		String maxkey=null;int maxvalue=-1;
		for(Text value:values){
			if(!map.containsKey(value.toString())){
				map.put(value.toString(), 1);
			}
			else{
				map.put(value.toString(), map.get(value.toString())+1);
			}
		}
		for(Entry<String, Integer> entry: map.entrySet()){
			if(entry.getValue()>maxvalue){
				maxkey=entry.getKey();
				maxvalue=entry.getValue();
			}
		}
		context.write(null, new Text(flower_name+" belongs to the species of "+maxkey));
	}
}
