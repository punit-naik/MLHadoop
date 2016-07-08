import java.util.ArrayList;
import java.util.LinkedHashSet;

public class get_unique_items{
	public ArrayList<Integer> get(ArrayList<String> vals, int a){
		ArrayList<Integer> items=new ArrayList<Integer>();
		ArrayList<Integer> unique_items=new ArrayList<Integer>();
		for(int i=0;i<a;i++){
			String[] tokens=vals.get(i).split("\\,");
			items.add(Integer.parseInt(tokens[1]));
		}
		unique_items=new ArrayList<Integer>(new LinkedHashSet<Integer>(items));
		return unique_items;
	}
}