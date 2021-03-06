package self.define.input;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import copy.from.textinputformat.MyTextInputFormat;

/**
 * Sample from  http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Purpose
 * Example: WordCount v1.0
 */
public class WordCountV3 {
          public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
          private final static IntWritable one = new IntWritable(1);
	      private Text word = new Text();
	
	      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	          word.set(tokenizer.nextToken());
	          output.collect(word, one);
	        }
	      }
	    }
	
	    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	        int sum = 0;
	        while (values.hasNext()) {
	          sum += values.next().get();
	        }
	        output.collect(key, new IntWritable(sum));
	      }
	    }
	
	    public static void main(String[] args) throws Exception {
	      JobConf conf = new JobConf(WordCountV3.class);
	      conf.setJobName("wordcount");
	
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class);
	
	      conf.setMapperClass(Map.class);
	      /*
	       * Users can optionally specify a combiner via JobConf.setCombinerClass(Class), 
	       * to perform local aggregation of the intermediate outputs, 
	       * which helps to cut down the amount of data transferred from the Mapper to the Reducer.
	       */
	      conf.setCombinerClass(Reduce.class);
	      conf.setReducerClass(Reduce.class);
	
	      /*
	       * How to Read calculate data and Write result.
	       * Here, use text way that read calculate data from HDFS and then write result
	       * back to HDFS
	       */
	      //conf.setInputFormat(MyTextInputFormat.class);
	      conf.setInputFormat(MyInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	 
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
          FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	      JobClient.runJob(conf);
	    }
	}
