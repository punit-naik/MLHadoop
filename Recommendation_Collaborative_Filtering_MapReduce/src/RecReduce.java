import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RecReduce extends Reducer<Text, Text, Text, Text>{
	public static String delimiter=null;
	@Override
	public void setup(Context context){
		delimiter=context.getConfiguration().get("delimiter");
	}
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int n=0;
		if(n==0){
			FileSystem hdfs= FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(context.getConfiguration().get("outFile")))));
			String line=null;
			while((line=br.readLine())!=null){
				String[] parts=line.replaceAll("\n", "").split(delimiter);
				if((key.toString().split(delimiter)[0]).contentEquals(parts[0])){
					n=Integer.parseInt(parts[1]);
					break;
				}
			}
			br.close();
			hdfs.close();
		}
		String[] value=null;
		double pref=0;
		HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
		HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
		for (Text val : values) {
			if(val.toString().contains(",")){
				value = val.toString().split(",");
				if (value[0].equals("A")) {
					for(int z=1;z<=n;z++){
						hashA.put(z, Float.parseFloat(value[z]));}
				} else{
					for(int a=1;a<=n;a++){
						hashB.put(a, Float.parseFloat(value[a]));}
				}
			}
			else{
				pref=Double.parseDouble(val.toString());
			}
		}
		float result = 0.0f;
		float a_ij;
		float b_jk;
		for (int j=1;j<=n;j++) {
			a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
			b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
			result +=a_ij*b_jk;
		}
		if(pref==0.0){
			context.write(null, new Text(key.toString() + ";" + Float.toString(result)));
		}
		//delimiter=null;
		n=0;
	}
}
