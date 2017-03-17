package ChinaData;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Qu12 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String parts[] = value.toString().split(";");
			String dayOfWeek = null;
			try {
				dayOfWeek = day(parts[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long sales = Long.parseLong(parts[8]);
			context.write(new Text(dayOfWeek),new LongWritable(sales));
		}

		private String day(String string) throws ParseException {
			SimpleDateFormat date = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
			Date day = date.parse(string);
			date.applyPattern("EEEE");
			String DayOfWeek = date.format(day);
			return DayOfWeek;
		}
	}
	public static class ReduceClass extends Reducer<Text,LongWritable,Text,Text>{
		TreeMap<Long,String> treemap = new TreeMap<>();
		long compsales = 0; 
		public void reduce(Text key,Iterable<LongWritable>value,Context context) throws IOException, InterruptedException{
			long totalSales = 0 ;
			for(LongWritable val:value){
				totalSales  += val.get();
			}
			
			compsales += totalSales;
			String output = key.toString()+","+totalSales;
			treemap.put(totalSales, output);
			//context.write(key,new DoubleWritable(totalSales));
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(String t:treemap.values()){
				String parts[] = t.split(",");
				double totalsales = Long.parseLong(parts[1]);
				double perc =(totalsales)/((double)compsales)*100;
				context.write(new Text(parts[0]),new Text(parts[1]+","+perc));
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Qu12");
		job.setJarByClass(Qu12.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
