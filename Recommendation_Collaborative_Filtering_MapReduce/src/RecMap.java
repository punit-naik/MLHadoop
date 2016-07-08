import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RecMap extends Mapper<LongWritable, Text, Text, Text> {
	public static String delimiter=null;
	public static String identifier=null;
	public static TreeMap<String,Integer> co_oc_mat=new TreeMap<String,Integer>();
	public static HashMap<String,Float> user_scoring_mat=new HashMap<String,Float>();
	public static TreeMap<String,Float> sorted_user_scoring_mat=new TreeMap<String,Float>();
	public static ArrayList<String> vals=new ArrayList<String>();
	public static ArrayList<Integer> unique_items=new ArrayList<Integer>();
	public static ArrayList<Integer> unique_users=new ArrayList<Integer>();
	public static int a=0;
	@Override
	public void setup(Context context){
		delimiter=context.getConfiguration().get("delimiter");
		identifier=context.getInputSplit()+delimiter;
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		++a;
		String b=value.toString();
		vals.add(b);
		String[] parts=b.split("\\,");
		user_scoring_mat.put(parts[0]+","+parts[1], Float.parseFloat(parts[2]));
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		co_oc_mat.putAll(new get_co_oc_mat().get(vals, a));
		unique_users.addAll(new get_unique_users().get(vals, a));
		unique_items.addAll(new get_unique_items().get(vals, a));
		FileSystem hdfs = FileSystem.get(context.getConfiguration());
		Path outFile=new Path(context.getConfiguration().get("outFile"));
		String line1="";
		if (!hdfs.exists(outFile)){
			OutputStream out = hdfs.create(outFile);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			br.write(identifier+unique_items.size()+"\n");
			br.close();
			hdfs.close();
		}
		else{
			String line2=null;
			BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(outFile)));
			while((line2=br1.readLine())!=null){
				line1=line1.concat(line2)+"\n";
			}
			br1.close();
			hdfs.delete(outFile, true);
			OutputStream out = hdfs.create(outFile);
			BufferedWriter br2 = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
			br2.write(line1+identifier+unique_items.size()+"\n");
			br2.close();
			hdfs.close();
		}
		for(int i=0;i<unique_users.size();i++){
			for(int j=0;j<unique_items.size();j++){
				if(!user_scoring_mat.containsKey(unique_users.get(i)+","+unique_items.get(j))){
					user_scoring_mat.put(unique_users.get(i)+","+unique_items.get(j), 0.0f);
				}
			}
		}
		sorted_user_scoring_mat.putAll(user_scoring_mat);
		String prev="null";int row_num=-1;String value="A";
		String prev2="null";int col_num=-1;String value2="B";

		//Transmitting co_oc_mat
		for(Entry<String, Integer> entry: co_oc_mat.entrySet()){
			String check_val=entry.getKey().split("\\,")[0];
			if(!prev.contentEquals(check_val)){
			// If code enters this block, it will mean that the row has changed
			// We have to transmit the aggregated values of the previous row and re-initialise the values.
				if(row_num==-1){
					prev=check_val;
					//++row_num;
					row_num=Integer.parseInt(check_val);
				}
				else{
					for(int i=0;i<unique_users.size();i++){
						String key=row_num+","+unique_users.get(i);
						//String key=row_num+","+i;
						context.write(new Text(identifier+key), new Text(value));
					}
					value="A";
					prev=check_val;
					++row_num;
				}
			}
			// Iterating through one row and fetching its values.
			// Joining them together in a string.
			// i.e. The row indices will be equal and we are currently traversing through the same row i.e. prev and check_val are equal
			value=value+","+entry.getValue();
		}
		// We have to transmit the aggregated values of the final row
		// since the matrix is fully iterated over and it won't enter the block where values are transmitted.
		for(int i=0;i<unique_users.size();i++){
			String key=row_num+","+unique_users.get(i);
			//String key=row_num+","+i;
			context.write(new Text(identifier+key), new Text(value));
		}

		//Transmitting sorted_user_scoring_mat
		for(Entry<String, Float> entry: sorted_user_scoring_mat.entrySet()){
			String check_val=entry.getKey().split("\\,")[0];
			if(!prev2.contentEquals(check_val)){
				// If code enters this block, it will mean that the column has changed
				// We have to transmit the aggregated values of the previous column and re-initialise the values.
				if(col_num==-1){
					prev2=check_val;
					//++col_num;
					col_num=Integer.parseInt(check_val);
				}
				else{
					for(int i=0;i<unique_items.size();i++){
						String key=unique_items.get(i)+","+col_num;
						//String key=i+","+col_num;
						context.write(new Text(identifier+key), new Text(value2));
					}
					value2="B";
					prev2=check_val;
					++col_num;
				}
			}
			// Iterating through one column and fetching its values.
			// Joining them together in a string.
			// i.e. The column indices will be equal and we are currently traversing through the same column i.e. prev2 and check_val are equal
			value2=value2+","+entry.getValue();
			// For an extra check at the RecReduce
			context.write(new Text(identifier+entry.getKey().split("\\,")[1]+","+entry.getKey().split("\\,")[0]), new Text(String.valueOf(entry.getValue())));
		}
		// We have to transmit the aggregated values of the final column
		// since the matrix is fully iterated over and it won't enter the block where values are transmitted.
		for(int i=0;i<unique_items.size();i++){
			String key=unique_items.get(i)+","+col_num;
			//String key=i+","+col_num;
			context.write(new Text(identifier+key), new Text(value2));
		}
	}
}
