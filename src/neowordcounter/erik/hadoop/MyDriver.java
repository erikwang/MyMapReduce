package neowordcounter.erik.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MyWordCount");
		job.setJarByClass(neowordcounter.erik.hadoop.MyDriver.class);
		job.setMapperClass(neowordcounter.erik.hadoop.MyMapper.class);
				
		//job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        //job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);


		job.setCombinerClass(neowordcounter.erik.hadoop.MyReducer.class);
		job.setReducerClass(neowordcounter.erik.hadoop.FinalReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//Example: $>hadoop jar ./my2.jar /tmp/myemails.txt /tmp/moutput9
		FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(new Integer(args[2]).intValue());
        
		//Hard code,make args into program 
		//FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop.advol:9000/tmp/myemails.txt"));
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop.advol:9000/tmp/output125"));

		if (!job.waitForCompletion(true))
			return;
	}
}