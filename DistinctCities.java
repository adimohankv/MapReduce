import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DistinctCities {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String city = parts[3];
			int score = Integer.parseInt(parts[2]);
			context.write(new Text(city), new IntWritable(score));
		}
	}
	
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int sum = 0;
			int counter = 0;
			for (IntWritable val : value){
				sum += val.get();
				counter++;
			}
			int avg = sum/counter;
			context.write(key, new IntWritable(avg));
		}
	}
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "Combiner");
		  job.setJarByClass(DistinctCities.class);
		  job.setMapperClass(MapClass.class);
		  job.setReducerClass(ReduceClass.class);
		  //job.setNumReduceTasks(0);
		  job.setCombinerClass(ReduceClass.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  //job.setOutputValueClass(NullWritable.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
	//for combiners do job.getCombiner(Reduce.class)
}
