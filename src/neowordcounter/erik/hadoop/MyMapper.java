package neowordcounter.erik.hadoop;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<Object, Text, Text, IntWritable> { //IN_KEY, IN_VALUE(a line of file), OUT_KEY(the word), OUT_VALUE()
	private static final IntWritable one = new IntWritable(1);
	private Text aword = new Text();
	private String pattern = "[^\\w]";
	
	@Override
	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String aline = ivalue.toString();
		aline = aline.replaceAll(pattern," "); //change any non-character a-zA-Z0-9 to space
		aline = aline.toLowerCase();
		System.out.println("aline size is"+aline.length()+" "+aline);
		StringTokenizer istr = new StringTokenizer(aline);
		while(istr.hasMoreTokens()){
			aword.set(istr.nextToken()); //extract this word to aword
			context.write(aword, one); //put a new element into output collect as aword => 1	
		}
	}
}