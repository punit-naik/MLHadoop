import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCentroidCalculationReduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int count=0;
		float x_sum=0, y_sum=0;
		for(Text val:values){
			StringTokenizer xy_points=new StringTokenizer(val.toString(),",");
			float x_point=Float.parseFloat(xy_points.nextToken());
			float y_point=Float.parseFloat(xy_points.nextToken());
			x_sum+=x_point;
			y_sum+=y_point;
			count++;
		}
		context.write(key,new Text((x_sum/count)+","+(y_sum/count)));
	}
}
