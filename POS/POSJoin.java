package POS;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class POSJoin {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		private Map<String,String> store_master = new HashMap<String,String>();
		
protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
			if (p.getName().equals("store_master")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String store_id = tokens[0];
						String state = tokens[2];
						store_master.put(store_id,state);
						line = reader.readLine();
					}
					reader.close();
				}
					
			if (store_master.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	String store_id = tokens[0];
        	
        	String state = store_master.get(store_id);
        	//String sal_desig =  + "," + desig; 
        	
      	  	context.write(value,new Text(state));
   
        } 
        
}
	
	public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {

Configuration conf = new Configuration();
conf.set("mapreduce.output.textoutputformat.separator", ",");
Job job = Job.getInstance(conf);
job.setJarByClass(POSJoin.class);
job.setJobName("Map Side Join");
job.setMapperClass(MapClass.class);
job.addCacheFile(new Path("store_master").toUri());
job.setNumReduceTasks(0);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);


}
}

	

