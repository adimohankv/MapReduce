import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class STDCalls {

 public static class MapClass extends Mapper < LongWritable, Text, LongWritable, IntWritable > {
  LongWritable phonenumber = new LongWritable();
  IntWritable durationInMinutes = new IntWritable();
  public void map(LongWritable key, Text value, Context context) {
   try {
    String[] parts = value.toString().split(",");
    if (parts[4].equals("1")) {
    	long phno = Long.parseLong(parts[0]);
     phonenumber.set(phno);
     String callStartTime = parts[2];
     String callEndTime = parts[3];
     long duration = toMillis(callEndTime) - toMillis(callStartTime);
     durationInMinutes.set((int)(duration / (1000 * 60)));
     context.write(phonenumber, durationInMinutes);

    }
   } catch (Exception e) {
    System.out.println(e.getMessage());
   }
  }

  long toMillis(String date) {
   SimpleDateFormat format = new SimpleDateFormat(
    "yyyy-MM-dd HH:mm:ss");
   Date dateFrm = null;
   try {
    dateFrm = format.parse(date);
   } catch (ParseException e) {
    e.printStackTrace();
   }
   return dateFrm.getTime();
  }

 }

 public static class ReduceClass extends Reducer < LongWritable, IntWritable, LongWritable, IntWritable > {
  private IntWritable result = new IntWritable();

  public void reduce(LongWritable key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {
   long sum = 0;
   for (IntWritable val: values)
    sum = sum + val.get();
   result.set((int) sum);
   if (sum >= 60)
    context.write(key, result);
  }

 }

 public static void main(String args[]) throws Exception {
  Configuration conf = new Configuration();
  conf.set("mapreduce.output.textoutputformat.separator", ",");
  Job job = Job.getInstance(conf, "STD Calls");
  job.setJarByClass(STDCalls.class);
  job.setMapperClass(MapClass.class);
  job.setReducerClass(ReduceClass.class);
  job.setReducerClass(ReduceClass.class);
  job.setOutputKeyClass(LongWritable.class);
  //job.setNumReduceTasks(0);
  job.setOutputValueClass(IntWritable.class);
  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);

 }

}