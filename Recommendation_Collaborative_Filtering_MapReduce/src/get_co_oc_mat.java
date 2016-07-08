import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;

public class get_co_oc_mat{
	public HashMap<String,Integer> get(ArrayList<String> vals, int a){
		HashMap<String,Integer> co_oc_mat=new HashMap<String,Integer>();
		ArrayList<Integer> items=new ArrayList<Integer>();
		ArrayList<Integer> unique_items=null;
		ArrayList<Integer> users=new ArrayList<Integer>();
		ArrayList<Integer> unique_users=null;
		for(int i=0;i<a;i++){
			String[] tokens=vals.get(i).split("\\,");
			users.add(Integer.parseInt(tokens[0]));
			items.add(Integer.parseInt(tokens[1]));
		}
		unique_users=new ArrayList<Integer>(new LinkedHashSet<Integer>(users));
		Collections.sort(unique_users);
		unique_items=new ArrayList<Integer>(new LinkedHashSet<Integer>(items));
		Collections.sort(unique_items);

		// Updating Diagonal Elements of co_oc_mat;
		for(int i=0;i<a;i++){
			String[] tokens=vals.get(i).split("\\,");
			String check=tokens[1]+","+tokens[1];
			if(!co_oc_mat.containsKey(check)){
				co_oc_mat.put(check, 1);
			}
			else{
				co_oc_mat.put(check, co_oc_mat.get(check)+1);
			}

		}

		// Updating the rest of the elements of co_oc_mat;
		for(int i=0;i<a-1;i++){
			String[] tokens1=vals.get(i).split("\\,");
			for(int j=1;j<a;j++){
				String[] tokens2=vals.get(j).split("\\,");
				if(tokens1[0].contentEquals(tokens2[0])){
					if(!tokens1[1].contentEquals(tokens2[1])){
						if(!co_oc_mat.containsKey(tokens1[1]+","+tokens2[1])){
							co_oc_mat.put(tokens1[1]+","+tokens2[1], 1);
							co_oc_mat.put(tokens2[1]+","+tokens1[1], 1);
						}
						else{
							co_oc_mat.put(tokens1[1]+","+tokens2[1], co_oc_mat.get(tokens1[1]+","+tokens2[1])+1);
							co_oc_mat.put(tokens2[1]+","+tokens1[1], co_oc_mat.get(tokens2[1]+","+tokens1[1])+1);
						}
					}
				}
				else{
					if(j-i>1){
						break;
					}
					else{
						i++;
						j++;
					}
				}
			}
		}

		// remaining elements are assigned to 0
		for(int i=0;i<unique_items.size();i++){
			for(int j=0;j<unique_items.size();j++){
				if(co_oc_mat.containsKey(unique_items.get(i)+","+unique_items.get(j))){
					continue;
				}
				else{
					co_oc_mat.put(unique_items.get(i)+","+unique_items.get(j), 0);
				}
			}
		}
		return co_oc_mat;
	}
}