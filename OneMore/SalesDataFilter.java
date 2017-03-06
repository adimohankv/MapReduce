package OneMore;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesDataFilter {

	public static class FilterMapper
       extends Mapper<LongWritable, Text, Text, NullWritable>{

    private Text sentence = new Text();
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String mySearchText = context.getConfiguration().get("myText");
    	String line = value.toString();
     
      if(mySearchText != null)
      {
    	  if(line.contains(mySearchText))
          {
    		  sentence.set(line);
    		  context.write(sentence, NullWritable.get());
         }
      }
    }
  }


  public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	//System.out.println(args.length);
	//System.out.println(args[2]);
	  if(args.length > 2)
      {
		  conf.set("myText", args[2]);
      }
	Job job = Job.getInstance(conf, "String Search");
    job.setJarByClass(SalesDataFilter.class);
    job.setMapperClass(FilterMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}