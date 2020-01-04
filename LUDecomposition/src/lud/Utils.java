package lud;

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
}
