import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.text.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataCleaning2 {

 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] Line_Token = line.split(",");
        String date = Line_Token[0];
        String location = Line_Token[1];
        String category = Line_Token[2];
    		double sum = 0;
        String average;
    		int count = 0;
        
        DecimalFormat df = new DecimalFormat("###0.##")
    		String Output_Line = date + "," + location + "," + category ;

    		for(int i=3 ; i<Line_Token.length ; i++){
            Line_Token[i] = Line_Token[i].replace("*","");
      			Line_Token[i] = Line_Token[i].replace("#","");
      			Line_Token[i] = Line_Token[i].replace("NR","0");

      			if(Line_Token[i].equals("")) continue;
      			else{
        				sum += Double.parseDouble(Line_Token[i]);
        				count += 1;
              }
        }
        if(count==0)  average = "0";
        else  average = df.format(sum/count);

        for(int i=3 ; i<Line_Token.length ; i++){
            if(Line_Token[i].equals(""))	Line_Token[i] = average;

      			String output = "," + Line_Token[i];
      			Output_Line = Output_Line.concat(output);
        }
        context.write(new Text(Output_Line), new Text());
    }
 }

 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

        context.write(key,new Text());
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

        Job job = new Job(conf, "datacleaning");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(DataCleaning.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }

}
