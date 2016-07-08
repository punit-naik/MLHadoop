import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansCentroidCalculationMap extends Mapper<LongWritable, Text, Text, Text>{
	public static HashMap<Integer,Float> map=new HashMap<Integer,Float>();
	public static Double minkey=(double) 0;
	public static int noc=0, dimension=0;
	public static ArrayList<Float> centers=new ArrayList<Float>();
	public static Double minvalue=Double.POSITIVE_INFINITY;
	public static float euc_dist(Float[] a, Float[] b,int num){
		float distance=0;
		float val=0;
		for(int i=0;i<num;i++){
			val+=((a[i]-b[i])*(a[i]-b[i]));
		}
		distance=(float) Math.sqrt(val);
		return distance;
	}
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		noc=Integer.parseInt(context.getConfiguration().get("noc"));
		dimension=Integer.parseInt(context.getConfiguration().get("dimension"));
		for(int i=0;i<noc*2;i++){
			centers.add(context.getConfiguration().getFloat("c"+i));
		}
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] points = value.toString().split("\\|");
		for(int i=0;i<points.length;i++){
			String[] str_point=points[i].split("\\,");
			Float[] xy_points= new Float[2];
			xy_points[0]=(Float.parseFloat(str_point[0]));
			xy_points[1]=(Float.parseFloat(str_point[1]));
			for(int j=0;j<noc*2;j+=2){
				Float[] d=new Float[2];
				d[0]=centers.get(j);
				d[1]=centers.get(j+1);
				map.put(j, euc_dist(xy_points,d,dimension));
			}
			for(Entry<Integer, Float> entry: map.entrySet()){
				if(entry.getValue()<minvalue){
					minkey=entry.getKey();
					minvalue=entry.getValue();
				}
			}
			map.clear();
			minvalue=Double.POSITIVE_INFINITY;
			context.write(new Text(centers.get(minkey)+","+centers.get(minkey+1)), new Text(xy_points[0]+","+xy_points[1]));
		}
	}
}
