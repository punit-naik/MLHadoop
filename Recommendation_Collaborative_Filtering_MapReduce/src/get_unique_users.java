import java.util.ArrayList;
import java.util.LinkedHashSet;

public class get_unique_users{
	public ArrayList<Integer> get(ArrayList<String> vals, int a){
		ArrayList<Integer> users=new ArrayList<Integer>();
		ArrayList<Integer> unique_users=new ArrayList<Integer>();
		for(int i=0;i<a;i++){
			String[] tokens=vals.get(i).split("\\,");
			users.add(Integer.parseInt(tokens[0]));
		}
		unique_users=new ArrayList<Integer>(new LinkedHashSet<Integer>(users));
		return unique_users;
	}
}