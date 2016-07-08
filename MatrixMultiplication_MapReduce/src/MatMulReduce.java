import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatMulReduce extends Reducer<Text, Text, Text, Text>{
	int n=0;
	@Override
	public void setup(Context context){
		n=Integer.parseInt(context.getConfiguration().get("n"));
	}
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		String[] value;
		HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
		HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
		for (Text val : values) {
			value = val.toString().split(",");
			if (value[0].equals("A")) {
				for(int z=1;z<=n;z++){
					hashA.put(z, Float.parseFloat(value[z]));}
			} else{
				for(int a=1;a<=n;a++){
					hashB.put(a, Float.parseFloat(value[a]));}
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
		context.write(null, new Text(key.toString() + "," + Float.toString(result)));
	}
}
