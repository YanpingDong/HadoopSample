package multiple.step.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RootJob extends Configured implements Tool{
	
	private static final Log LOG = LogFactory.getLog(RootJob.class);
	
	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = new JobConf(getConf(), RootJob.class);
		conf.setJobName("wordcountv2");

	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class);

	      conf.setMapperClass(StepOneMap.class);
	      /*
	       * Users can optionally specify a combiner via JobConf.setCombinerClass(Class), 
	       * to perform local aggregation of the intermediate outputs, 
	       * which helps to cut down the amount of data transferred from the Mapper to the Reducer.
	       */
	      conf.setCombinerClass(StepOneReduce.class);
	      conf.setReducerClass(StepOneReduce.class);
	      /*
	       * How to Read calculate data and Write result.
	       * Here, use text way that read calculate data from HDFS 
	       * and then write result back to HDFS
	       */
	      conf.setInputFormat(TextInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	      FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
	      
	      RunningJob runningJob = JobClient.runJob(conf);
	      
	      
	      while(!runningJob.isComplete())
	      {
	    	  Thread.sleep(1000);
	    	  LOG.info("wait for first job complete");
	      }
	      /*
	       * If first MR complete and successful then to start second MR.
	       * You've already found that EACH MR step mush have to read from HDFS and when finished to stored result to HDFS,
	       * So this process is waste time, Spark optimize this process
	       */
	      if(runningJob.isSuccessful())
	      {
	      	  JobConf conf2 = new JobConf(getConf(), RootJob.class);
		  conf2.setJobName("wordcountv2");

		  conf2.setOutputKeyClass(Text.class);
		  conf2.setOutputValueClass(IntWritable.class);

		  conf2.setMapperClass(StepOneMap.class);
		  /*
		  * Users can optionally specify a combiner via JobConf.setCombinerClass(Class), 
		  * to perform local aggregation of the intermediate outputs, 
		  * which helps to cut down the amount of data transferred from the Mapper to the Reducer.
		  */
		  conf2.setCombinerClass(StepOneReduce.class);
		  conf2.setReducerClass(StepOneReduce.class);
		  /*
		  * How to Read calculate data and Write result.
		  * Here, use text way that read calculate data from HDFS 
		  * and then write result back to HDFS
		  */
		  conf2.setInputFormat(TextInputFormat.class);
		  conf2.setOutputFormat(TextOutputFormat.class);
		  FileInputFormat.setInputPaths(conf2, new Path(arg0[0]));
		  FileOutputFormat.setOutputPath(conf2, new Path(arg0[1]+"1"));
	    	  RunningJob runningJob2 = JobClient.runJob(conf2);
	    	  return 0;
	      }
		     
	      else
	    	  return 1;
	}

	public static void main(String[] args) throws Exception {
		 int res = ToolRunner.run(new Configuration(), new RootJob(), args);
	      
	      System.exit(res);
	}
}
