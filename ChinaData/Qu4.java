package ChinaData;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Qu4 {
public static class TopbuyerMap extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(";");
			String prodid = parts[5];
			double saleamount = Double.parseDouble(parts[8]);
			double costamount = Double.parseDouble(parts[7]);
			String qu = parts[6];
			String join = saleamount+","+costamount+","+qu;
			context.write(new Text(prodid),new Text(join));
		}		
	}
public static class TopbuyerReducer extends Reducer<Text,Text,NullWritable,Text>{
	private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
	public void reduce (Text key,Iterable<Text> value,Context context ) throws IOException, InterruptedException{
		double salesum=0;
		double costsum = 0;
		int qusum = 0;
		for (Text val:value){
			String[] parts = val.toString().split(",");
			double saleamount = Double.parseDouble(parts[0]);
			double costamount = Double.parseDouble(parts[1]);
			int qu = Integer.parseInt(parts[2]);
			salesum += saleamount;
			costsum += costamount;
			qusum += qu;			
		}
		double profit = salesum-costsum;
		double margin = (profit/costsum)*100;
		String keys = key.toString();
		String output = keys+","+margin+","+profit+","+","+qusum;
		repToRecordMap.put( new Double(margin), new Text (output));
//		if (repToRecordMap.size() > 10) {
//					repToRecordMap.remove(repToRecordMap.firstKey());
//				}
	}
	
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			
		for (Text t : repToRecordMap.values()) {
			// Output max amt record to the file system with a null key
			context.write(NullWritable.get(), t);
			}
}
		
	}
public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Top Record for largest amount spent");
    job.setJarByClass(Qu4.class);
    job.setMapperClass(TopbuyerMap.class);
    job.setReducerClass(TopbuyerReducer.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


