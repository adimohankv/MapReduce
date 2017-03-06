package OneMore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class TextSearch {
	public static class Mapclass extends Mapper<LongWritable,Text,Text,FloatWritable>{
		Text sentence = new Text();
		FloatWritable amtf = new FloatWritable();
		public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String mySearchText = context.getConfiguration().get("myText");
			String line = value.toString();
			String[] parts = value.toString().split(",");
			float amt = Float.parseFloat(parts[3]);
			if(mySearchText != null){
				if(line.contains(mySearchText)){
					sentence.set(parts[2]);
					amtf.set(amt);
					context.write(sentence,amtf);
			    }
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		FloatWritable result = new FloatWritable();
		public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
			float sum = 0;
			for(FloatWritable val:values){
				sum += val.get();
			}
			
			context.write(key,new FloatWritable(sum));
		}
	}
	
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  System.out.println(args.length);
		  System.out.println(args[2]);
		  
		  if(args.length > 2){
			  conf.set("myText",args[2]);
		  }
		  Job job = Job.getInstance(conf, "TextSearch");
		  job.setJarByClass(TextSearch.class);
		  job.setMapperClass(Mapclass.class);
		  job.setReducerClass(ReduceClass.class);
		  //job.setNumReduceTasks(0);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(FloatWritable.class);
		  //job.setOutputValueClass(NullWritable.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
}
