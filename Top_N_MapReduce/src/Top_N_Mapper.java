import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top_N_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public static Map<String, Integer> sortByComparator(Map<String, Integer> m){
		List<Entry<String,Integer>> list=new LinkedList<Entry<String,Integer>>(m.entrySet());
		Collections.sort(list, new Comparator<Entry<String,Integer>>(){
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return -(o1.getValue().compareTo(o2.getValue()));
			}
		});
		Map<String,Integer> sortedMap=new LinkedHashMap<String,Integer>();
		for(Iterator<Entry<String,Integer>> it=list.iterator(); it.hasNext();){
			Entry<String,Integer> e=it.next();
			sortedMap.put(e.getKey(), e.getValue());
		}
		return sortedMap;
	}
	public static Map<String,Integer> sm=new HashMap<String,Integer>();
	public static int N=0;
	@Override
	public void setup(Context context){
		N=Integer.parseInt(context.getConfiguration().get("N"));
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values=value.toString().split(",");
		if(sm.containsKey(values[0]))
			sm.put(values[0], sm.get(values[0])+Integer.parseInt(values[1]));
		else
			sm.put(values[0], Integer.parseInt(values[1]));
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		int count=0;
		// Sorting based on values descendingly
		Map<String,Integer> p=sortByComparator(sm);
		Map<String,Integer> x=new LinkedHashMap<String,Integer>();
		for(Entry<String,Integer> e:p.entrySet()){
			if(count<=N){
				x.put(e.getKey(), e.getValue());
				count++;
			}
			else
				break;
		}
		context.write(new Text("1"), new Text(x.toString()));
		sm.clear();
	}
}
