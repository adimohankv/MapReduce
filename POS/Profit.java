package POS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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


public class Profit {
	public static class MapClass extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		Map<String,String> tp = new HashMap<String,String>();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			URI[] files = context.getCacheFiles();
			Path p = new Path(files[0]);
			if (p.getName().equals("cost-sheet.txt")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split(",");
					String item_id = tokens[0];
					String cp = tokens[1];
					tp.put(item_id,cp);
					line = reader.readLine();
				}
				reader.close();
			}	
			if (tp.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}
		}
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String item_id = parts[1];
			int sp = Integer.parseInt(parts[3]);
			int cp = Integer.parseInt(tp.get(item_id));
			int quantity = Integer.parseInt(parts[2]);
			int profit = (sp-cp)*quantity;
			int item = Integer.parseInt(item_id);
			context.write(new IntWritable(item), new IntWritable(profit));
		}
	}
	
	public static class ReduceClass extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce (IntWritable key,Iterable<IntWritable> value,Context context ) throws IOException, InterruptedException{
			int totalprofit = 0;
			for (IntWritable val:value){
				totalprofit += val.get();
			}
			context.write(key,new IntWritable(totalprofit));
		}

	}
	public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(Profit.class);
		job.setJobName("Map Side Join");
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.addCacheFile(new Path("cost-sheet.txt").toUri());
		//job.setNumReduceTasks(0);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
