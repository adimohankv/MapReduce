package ChinaData;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Qu7 {
	public static class MapClass1 extends Mapper <LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			String proid = parts[5];
			String sales = "jan\t"+parts[8];
			context.write(new Text(proid),new Text(sales));
		}
	}
	public static class MapClass2 extends Mapper <LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			String proid = parts[5];
			String sales = "feb\t"+parts[8];
			context.write(new Text(proid),new Text(sales));
		}
	}
	public static class MapClass3 extends Mapper <LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			String proid = parts[5];
			String sales = "nov\t"+parts[8];
			context.write(new Text(proid),new Text(sales));
		}
	}
	public static class MapClass4 extends Mapper <LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			String proid = parts[5];
			String sales = "dec\t"+parts[8];
			context.write(new Text(proid),new Text(sales));
		}
	}
	
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			long jansum=0;
			long febsum = 0;
			long novsum = 0;
			long decsum = 0;
			for(Text val:value){
				String[] parts =val.toString().split("\t");
				if(parts[0].equals("jan")){
					jansum += Integer.parseInt(parts[1]);
				}
				else if (parts[0].equals("feb")){
					febsum += Integer.parseInt(parts[1]);
				}
				else if(parts[0].equals("nov")){
					novsum += Integer.parseInt(parts[1]);
				}
				else if (parts[0].equals("dec")){
					decsum += Integer.parseInt(parts[1]);
				} 
			}
			String output = jansum+";"+febsum+";"+novsum+";"+decsum;
			context.write(key,new Text(output));
		}
	}
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(Qu7.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MapClass1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MapClass2.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, MapClass3.class);
		MultipleInputs.addInputPath(job, new Path(args[3]),TextInputFormat.class, MapClass4.class);
		
		Path outputPath = new Path(args[4]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
