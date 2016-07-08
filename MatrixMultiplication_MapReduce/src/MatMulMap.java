import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MatMulMap extends Mapper<LongWritable, Text, Text, Text> {
	public static int m=0,n=0,p=0;
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		m = Integer.parseInt(context.getConfiguration().get("m"));
		n = Integer.parseInt(context.getConfiguration().get("n"));
		p = Integer.parseInt(context.getConfiguration().get("p"));
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text Key = new Text();
		Text Value = new Text();
		String line = value.toString();
		String[] val = line.split("\\,");
		if(val[0].contentEquals("A")){
			for(int x=0;x<p;x++){
				String s="";
				Key.set(val[val.length-1]+","+x);
				for(int i=0;i<val.length-1;i++){
					if(i<val.length-2){
						s=s.concat(val[i]+",");
					}
					else{
						s=s.concat(val[i]);
					}
				}
				Value.set(s);
				context.write(Key, Value);
			}
		}
		else{
			for(int y=0;y<m;y++){
				String s="";
				Key.set(y+","+val[val.length-1]);
				for(int i=0;i<val.length-1;i++){
					if(i<val.length-2){
						s=s.concat(val[i]+",");
					}
					else{
						s=s.concat(val[i]);
					}
				}
				Value.set(s);
				context.write(Key, Value);
			}
		}
	}
}
