package ChinaData;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Qu3a {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(";");
			String output = parts[5]+","+parts[2]+","+parts[7]+","+parts[8];
			context.write(new Text(parts[5]), new Text(output));
		}
	}
	public static class PartClass extends Partitioner<Text,Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] parts = value.toString().split(",");
			String age = parts[1].trim();
			if (age.equals("A")){
				return 0;
			}
			else if (age.equals("B")){
				return 1;
			}
			else if (age.equals("C")){
				return 2;
			}
			else if (age.equals("D")){
				return 3;
			}
			else if (age.equals("E")){
				return 4;
			}
			else if (age.equals("F")){
				return 5;
			}
			else if (age.equals("G")){
				return 6;
			}
			else if (age.equals("H")){
				return 7;
			}
			else if (age.equals("I")){
				return 8;
			}
			else return 9;
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>{
		private TreeMap<Long,Text> viable = new TreeMap<>();
		public void reduce(Text key,Iterable<Text> value ,Context context){
			long totalprofit =0;
			String prodid = "";
			String age ="";
			for (Text val : value){
				String part[] = val.toString().split(",");
				int sale = Integer.parseInt(part[3]);
				int cost = Integer.parseInt(part[2]);
				int profit = sale - cost;
				totalprofit += profit;
				prodid = part[0];
				age = part[1];
			}
			
			String output = prodid+","+age+","+totalprofit;
			viable.put(totalprofit,new Text(output));
			if(viable.size()>5){
				viable.remove(viable.firstKey());
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Text t:viable.descendingMap().values()){
				context.write(NullWritable.get(), t);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Record for largest amount spent");
	    job.setJarByClass(Qu3a.class);
	    job.setMapperClass(MapClass.class);
	    job.setPartitionerClass(PartClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
