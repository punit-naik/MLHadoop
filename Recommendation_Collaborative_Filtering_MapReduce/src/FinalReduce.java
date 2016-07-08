import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReduce extends Reducer<Text, Text, Text, Text>{
	String delimiter=null,identifier=null;
	@Override
	public void setup(Context context){
		delimiter=context.getConfiguration().get("delimiter");
		identifier=context.getTaskAttemptID().getTaskID().getId()+delimiter;
	}
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text val: values){
			context.write(new Text(/*identifier+*/key.toString()), val);//new Text(val.toString().split("\\,")[1]));
		}
	}

}
