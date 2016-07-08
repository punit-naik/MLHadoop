import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MBA_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static int group_num = 2;
	@Override
	public void setup(Context context){
		group_num=Integer.parseInt(context.getConfiguration().get("group_num"));
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals=value.toString().split("\\,");
		Arrays.sort(vals);
		if(vals.length>=group_num){
			for(int i=0;i<vals.length-(group_num-1);i++){
				String pair="";
				for(int j=0;j<group_num;j++){
					if(j==group_num-1)
						pair=pair+vals[i+j];
					else
						pair=pair+vals[i+j]+",";
				}
				context.write(new Text(pair), new IntWritable(1));
			}
		}
	}
}
