import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

public class gen_mutual_friends_matrix {
	public static TreeMap<String,ArrayList<String>> list=new TreeMap<String,ArrayList<String>>();
	public TreeMap<String,ArrayList<String>> generate(TreeMap<String,ArrayList<String>> x){
		for(Entry<String, ArrayList<String>> s1:x.entrySet()){
			for(Entry<String, ArrayList<String>> s2:x.entrySet()){
				if(!s1.getKey().contentEquals(s2.getKey()) && Integer.parseInt(s2.getKey())>Integer.parseInt(s1.getKey())){
					ArrayList<String> mutual=s1.getValue();
					mutual.retainAll(s2.getValue());
					list.put(s1.getKey()+","+s2.getKey(), mutual);
				}
			}
		}
		return list;
	}
}