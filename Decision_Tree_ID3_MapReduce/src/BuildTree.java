import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class BuildTree{
	public static int feat_count=0;
	public static LinkedHashMap<String,String> p=new LinkedHashMap<String,String>();
	public static HashMap<String,Integer> nodes=new HashMap<String,Integer>();
	public static HashMap<Integer,Double> gain= new HashMap<Integer,Double>();
	public static HashMap<Integer,String> intermediate= new HashMap<Integer,String>();
	public static HashMap<String,Double> feature_count= new HashMap<String,Double>();
	public static HashMap<String,Double> outcome_count= new HashMap<String,Double>();

	public static String[] getMax_m(HashMap<String,Double> x){
		String maxKey="";
		Double maxValue=Double.NEGATIVE_INFINITY;
		for(Entry<String,Double> e:x.entrySet()){
			if(e.getValue()>maxValue){
				maxKey=e.getKey();
				maxValue=e.getValue();
			}
		}
		String[] s=new String[2];
		s[0]=String.valueOf(maxKey);
		s[1]=String.valueOf(maxValue);
		return s;
	}

	public static String[] getMax(HashMap<Integer,Double> x){
		int maxKey=-1;
		Double maxValue=Double.NEGATIVE_INFINITY;
		for(Entry<Integer,Double> e:x.entrySet()){
			if(e.getValue()>maxValue && !nodes.containsKey(String.valueOf(e.getKey()))){
				maxKey=e.getKey();
				maxValue=e.getValue();
			}
		}
		String[] s=new String[2];
		s[0]=String.valueOf(maxKey);
		s[1]=String.valueOf(maxValue);
		return s;
	}

	public static LinkedHashMap<String, String> build(LinkedHashMap<String, String> g,ArrayList<String> data,int size){
		if(p.size()==0)
			p.putAll(g);
		if(feat_count==0)
			feat_count=data.get(0).split("\\,").length-1;

		for(int i=0;i<data.size();i++){
			String[] input=data.get(i).split("\\,");
			for(int q=0;q<input.length;q++){
				if(q==input.length-1){
					if(outcome_count.containsKey(input[q]))
						outcome_count.put(input[q], outcome_count.get(input[q])+1);
					else
						outcome_count.put(input[q], (double) 1);
				}
				else{
					if(feature_count.containsKey(q+","+input[q]+","+input[input.length-1]))
						feature_count.put(q+","+input[q]+","+input[input.length-1], feature_count.get(q+","+input[q]+","+input[input.length-1])+1);
					else
						feature_count.put(q+","+input[q]+","+input[input.length-1], (double) 1);
				}
			}
		}

		for(Entry<String,Double> e:feature_count.entrySet()){
			String[] key=e.getKey().split("\\,");
			if(intermediate.containsKey(Integer.parseInt(key[0])))
				intermediate.put(Integer.parseInt(key[0]), intermediate.get(Integer.parseInt(key[0]))+","+key[1]+":"+(e.getValue()));
			else
				intermediate.put(Integer.parseInt(e.getKey().split("\\,")[0]), String.valueOf(key[1]+":"+e.getValue()));
		}
		// Calculating the entropy of the whole Set.
		double entropy=0.0;
		for(Entry<String,Double> e:outcome_count.entrySet()){
			double p=((e.getValue()/size));
			entropy+=-(p*(Math.log(p)/Math.log(2)));
		}

		// Initialising the gain Map with all the keys
		// and the initial information gain which is ofcourse
		// the entropy of whole Set.
		for(int i=0;i<data.get(0).split("\\,").length-1;i++){
			gain.put(i, entropy);
		}
		for(Entry<Integer,String> e:intermediate.entrySet()){
			if(gain.containsKey(e.getKey())){
				double info_gain_except_the_entropy=0.0;
				String[] counts=e.getValue().split("\\,");
				HashMap<String,String> feat=new HashMap<String,String>();
				for(int j=0;j<counts.length;j++){
					if(feat.containsKey(counts[j].split("\\:")[0]))
						feat.put(counts[j].split("\\:")[0], feat.get(counts[j].split("\\:")[0])+","+counts[j].split("\\:")[1]);
					else
						feat.put(counts[j].split("\\:")[0], counts[j].split("\\:")[1]);
				}
				for(Entry<String,String> r:feat.entrySet()){
					String[] c=r.getValue().split("\\,");
					int num=0;
					for(int x=0;x<c.length;x++){
						num+=Double.parseDouble(c[x]);
					}
					double ent=0.0;
					for(int k=0;k<c.length;k++){
						double p=(Double.parseDouble(c[k])*Math.pow(num, -1));
						ent+=-(p*(Math.log(p)/Math.log(2)));
					}
					double cs=num*Math.pow(size, -1);
					info_gain_except_the_entropy+=ent * cs;
				}
				gain.put(e.getKey(), gain.get(e.getKey())-info_gain_except_the_entropy);
			}
		}
		String key=getMax(gain)[0];
		nodes.put(key, 1);
		HashMap<String,Double> test=new HashMap<String,Double>();
		for(Entry<String,Double> z:feature_count.entrySet()){
			String[] parts=z.getKey().split("\\,");
			if(parts[0].contentEquals(key)){
				if(test.containsKey(parts[1]+";"+parts[2]))
					test.put(parts[1]+";"+parts[2], test.get(parts[1]+";"+parts[2])+1);
				else
					test.put(parts[1]+";"+parts[2], (double) 1);
			}
		}
		String return_value=(key+","+getMax_m(test)[0]);
		HashMap<String,String> ret=new HashMap<String,String>();
		ret.put(key, getMax_m(test)[0]);
		if(p.containsKey(key))
			p.put(key, p.get(key)+"|"+getMax_m(test)[0]);
		else
			p.put(key, getMax_m(test)[0]);
		ArrayList<String> indices=new ArrayList<String>();
		for(int i=0;i<data.size();i++){
			String[] vals=data.get(i).split("\\,");
			String[] check=return_value.split("\\,")[1].split("\\;");
			if(vals[Integer.parseInt(key)].contentEquals(check[0]) && vals[vals.length-1].contentEquals(check[1]))
				indices.add(data.get(i));
		}

		// Removing the data points whose output has been decided
		data.removeAll(indices);
		
		// Clearing the global variables so that no data duplication occurs
		// when the values are passed to the recursion process
		// and thereby avoiding infinite recursion.
		gain.clear();
		intermediate.clear();
		feature_count.clear();
		outcome_count.clear();
		
		// In this example I don't loop util the dataset is empty
		// which should be the done.
		// Still it gets my work (building the decision tree) done though.
		if(data.size()==0 || nodes.size()==feat_count){
			String[] tbr=return_value.split("\\,");
			test.remove(tbr[1]);
			HashMap<String,Double> test2=new HashMap<String,Double>();
			for(Entry<String,Double> E:test.entrySet()){
				if(test2.containsKey(E.getKey().split("\\;")[0]))
					test2.put(E.getKey().split("\\;")[0], test2.get(E.getKey().split("\\;")[0])+1);
				else
					test2.put(E.getKey().split("\\;")[0], (double) 1);
			}
			Iterator<Entry<String, Double>> it1=test.entrySet().iterator(),it2=test2.entrySet().iterator();
			while (it1.hasNext() && it2.hasNext()){
				Map.Entry<String, Double> pairs1=(Entry<String,Double>) it1.next();
				Map.Entry<String, Double> pairs2=(Entry<String,Double>) it2.next();

				if(p.containsKey(key))
					if(pairs2.getValue()==(double) 1)
						p.put(key, p.get(key)+"|"+pairs1.getKey());
					else
						p.put(key, p.get(key)+"|"+pairs2.getKey());
				else
					if(pairs2.getValue()==(double) 1)
						p.put(key, pairs1.getKey());
					else
						p.put(key, pairs2.getKey());
			}
			int r=0;
			String vl="";
			for(Entry<String,String> n:p.entrySet()){
				++r;
				if(r==p.size()){
					String[] i=n.getValue().split("\\|");
					int count=i.length-1;
					for(int v=0;v<count;v++){
						if(v==count-1)
							vl+=i[v];
						else
							vl+=i[v]+"|";
					}
					p.put(n.getKey(), vl);
				}
			}
			return p;
		}
		else{
			String[] tbr=return_value.split("\\,");
			test.remove(tbr[1]);
			HashMap<String,Double> test2=new HashMap<String,Double>();
			for(Entry<String,Double> E:test.entrySet()){
				if(test2.containsKey(E.getKey().split("\\;")[0]))
					test2.put(E.getKey().split("\\;")[0], test2.get(E.getKey().split("\\;")[0])+1);
				else
					test2.put(E.getKey().split("\\;")[0], (double) 1);
			}
			Iterator<Entry<String, Double>> it1=test.entrySet().iterator(),it2=test2.entrySet().iterator();
			while (it1.hasNext() && it2.hasNext()){
				Map.Entry<String, Double> pairs1=(Entry<String,Double>) it1.next();
				Map.Entry<String, Double> pairs2=(Entry<String,Double>) it2.next();

				if(p.containsKey(key))
					if(pairs2.getValue()==(double) 1)
						p.put(key, p.get(key)+"|"+pairs1.getKey());
					else
						p.put(key, p.get(key)+"|"+pairs2.getKey());
				else
					if(pairs2.getValue()==(double) 1)
						p.put(key, pairs1.getKey());
					else
						p.put(key, pairs2.getKey());
			}
			return build(p,data,data.size());
		}
	}
}