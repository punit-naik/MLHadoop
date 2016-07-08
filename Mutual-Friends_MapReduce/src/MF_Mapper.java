import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MF_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public static TreeMap<String,ArrayList<String>> Friends=new TreeMap<String,ArrayList<String>>();
	public static ArrayList<String> ArrToList (ArrayList<String> l, String[] a){
		for(String i:a)
			l.add(i);
		return l;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals=value.toString().split("\\,");
		ArrayList<String> al=ArrToList(new ArrayList<String>(),vals[1].split(" "));
		Collections.sort(al);
		Friends.put(vals[0],al);
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(Entry<String,ArrayList<String>> s:new gen_mutual_friends_matrix().generate(Friends).entrySet())
			context.write(new Text(s.getKey()), new Text(s.getValue().toString().replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "")));
	}
}
