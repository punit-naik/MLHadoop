import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DT_ID3_Map extends Mapper<LongWritable, Text, Text, Text>{
	public static int count=0;
	public static ArrayList<String> input=new ArrayList<String>();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		input.add(value.toString());
		++count;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		LinkedHashMap<String,String> g = new LinkedHashMap<String,String>();
		LinkedHashMap<String,String> t=BuildTree.build(g,input, count);
		String key="";
		int c=0;
		for(Entry<String,String> T:t.entrySet()){
			++c;
			key=T.getKey()+","+T.getValue();
			System.out.println("key: "+key+" c: "+c);
			context.write(new Text(String.valueOf(c)), new Text(key));
		}
	}
}