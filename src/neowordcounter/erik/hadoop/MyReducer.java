package neowordcounter.erik.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	LinkedHashMap<Integer,String> myar;
	Map<Integer, String> sortedMapByKeys;
	int grandTotal = 0;
	protected void setup(Context context){
		myar = new LinkedHashMap<Integer,String>();
		sortedMapByKeys= new TreeMap<Integer,String>();
	} 
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		int sum = 0;
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()) {
			sum = sum + it.next().get();
		}
		context.write(key,new IntWritable(sum));;
		//System.out.println("Put a key=>value"+sum+"=>"+key.toString()+" to hashtable");
		myar.put(sum, key.toString());
		sortedMapByKeys.put(sum,key.toString());
		grandTotal += sum;
	}
	
	/*protected void cleanup(Context context){
		System.out.println("Sorted Map in Java by key using TreeMap : " + sortedMapByKeys);
		System.out.println("Not Sorted Map : "+myar);
		System.out.println("Grand Total of words : "+grandTotal);
		
		try {
			context.write(new Text(sortedMapByKeys.toString()),new IntWritable(99999));
			context.write(new Text("Grand Total of words: "),new IntWritable(grandTotal));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
}