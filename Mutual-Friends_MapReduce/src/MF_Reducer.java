import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MF_Reducer extends Reducer<Text, Text, Text, Text> {
	public static ArrayList<String> ArrToList (ArrayList<String> l, String[] a){
		for(String i:a)
			l.add(i);
		return l;
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> mutual_friends=new ArrayList<String>();
		for(Text value:values){
			if(!value.toString().contentEquals("")){
				String[] vals=value.toString().split("\\,");
				ArrToList(mutual_friends,vals);
			}
		}
		HashSet<String> hs=new HashSet<String>(mutual_friends);
		if(hs.size()>0)
			context.write(key, new Text(hs.toString().replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "")+"|"+hs.size()));
		else
			context.write(key, new Text("null"));
	}
}
