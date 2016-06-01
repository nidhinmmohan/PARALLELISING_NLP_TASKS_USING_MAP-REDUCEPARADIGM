//Part-of-Speech Tagging

package pos;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.StringTokenizer;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

	public class wordpos {

		public static String modelPath = null;
		public static class TokenizerMapper 
	     extends Mapper<Object, Text, Text, IntWritable>{
			
	  private final static IntWritable one = new IntWritable(1);
	 private Text word = new Text();
	    
	  public void map(Object key, Text value, Context context
	                  ) throws IOException, InterruptedException {
		  
		  InputStream modelIn = null;
		  String[] sentences = null;
		  modelIn = FileSystem.get(context.getConfiguration()).open(new Path("/home/hduser/workspace/OpenNlpTest/bin/en-sent.bin"));

			SentenceModel model = new SentenceModel(modelIn);
			SentenceDetectorME sdetector = new SentenceDetectorME(model);
		 
		 sentences = sdetector.sentDetect(value.toString());
	    	
		for(String sent:sentences){
			 word.set(sent.toString());
			
	      context.write(word, one);
	    }
	  }
	}
		 
		public static class IntSumReducer extends
				Reducer<Text, IntWritable, Text, IntWritable> {
			private IntWritable result = new IntWritable();
			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				
				System.out.println(key);
				String tag_sent = POSTag(key.toString());
				Text key1 = new Text();
					result.set(0);
					key1.set("\n"+tag_sent);
					context.write(key1, result);
				
				
				
			}
			
			public static String POSTag(String input) throws IOException {
				
				POSModel model = new POSModelLoader().load(new File("/home/hduser/workspace/OpenNlpTest/bin/en-pos-maxent.bin"));
				PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
				POSTaggerME tagger = new POSTaggerME(model);
			 
				ObjectStream<String> lineStream = new PlainTextByLineStream(
						new StringReader(input));
			 
				perfMon.start();
				String line;
				POSSample sample = null;
				while ((line = lineStream.read()) != null) {
			 
					String whitespaceTokenizerLine[] = WhitespaceTokenizer.INSTANCE
							.tokenize(line);
					String[] tags = tagger.tag(whitespaceTokenizerLine);
			 
					sample = new POSSample(whitespaceTokenizerLine, tags);
					System.out.println(sample.toString());
			 
					perfMon.incrementCounter();
				}
				perfMon.stopAndPrintFinalResult();
				return sample.toString();
			}
			
		}
		
		

		
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			
			
			
			Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    modelPath = "/user/hduser/models";
			  System.out.println(args.length);
		  
		    
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: POSTag <in> <out> <model>");
		      System.exit(2);
		    }
		    Job job = new Job(conf, "POSTag");
		    job.setJarByClass(HadoopPOSv3.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
		  }
		
			

		}


	


