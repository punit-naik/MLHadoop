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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top_N_Reducer extends Reducer<Text, Text, Text, Text> {
	public static Map<String,Integer> m=new HashMap<String,Integer>();
	public static int N=0;
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
	@Override
	public void setup(Context context){
		N=Integer.parseInt(context.getConfiguration().get("N"));
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text value:values){
			String val=StringUtils.substringBetween(value.toString(),"{","}");
			String[] key_val=val.split(",");
			for(String pair:key_val){
				String[] entry=pair.split("=");
				if(m.containsKey(entry[0].trim()))
					m.put(entry[0].trim(), m.get(entry[0].trim())+Integer.parseInt(entry[1].trim()));
				else
					m.put(entry[0].trim(), Integer.parseInt(entry[1].trim()));
			}
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		// Sorting based on values descendingly
		Map<String,Integer> x=sortByComparator(m);
		int count=0;
		for(Entry<String,Integer> e:x.entrySet()){
			if(count<N){
				context.write(new Text(e.getKey()+","+e.getValue()), null);
				count++;
			}
			else
				break;
		}
		m.clear();
	}
}
