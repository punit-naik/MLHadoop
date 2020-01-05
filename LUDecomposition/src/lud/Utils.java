package lud;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	
	public static String arrayToCSV(Double[] nVal2) {
        String result = "";

        if (nVal2.length > 0) {
            StringBuilder sb = new StringBuilder();

            for (Double s : nVal2) {
                sb.append(s).append(",");
            }

            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }
	
	public static Double[] stringToDoubleArray(String[] a) {
		Double[] x = new Double[a.length];
		for(int i = 0; i < a.length ; i++)
			x[i] = Double.valueOf(a[i]);
		return x;
	}

	public static void storeToHDFS(String data, String output, Configuration conf) throws IOException {
		
		FileSystem hdfs=FileSystem.get(conf);
		Path find_nth_row_output_path = new Path(conf.get("find_nth_row_output"));
		try {
		    if (hdfs.exists(find_nth_row_output_path)) {
		        hdfs.delete(find_nth_row_output_path, true);
		    }
		    DataOutputStream outStream = hdfs.create(find_nth_row_output_path);
		    BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(outStream, "UTF-8" ) );
		    bw.write(data);
		    bw.close();
		    hdfs.close();
		    outStream.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public static String readFromHDFS(String path, Configuration conf) throws IOException {
		
		FileSystem hdfs=FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(path))));
		String records = br.readLine().trim();
		br.close();
		hdfs.close();
		
		return records;
	}
}
