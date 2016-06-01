//Sentiment Analysis

package namedentity;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Sentiment{

	private static String modelPath = ("/user/hduser/models");

	 public static class TokenizerMapper 
     extends Mapper<Object, Text, Text, IntWritable>{
  
  private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {

		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		
		Annotation annotation = pipeline.process(value.toString());
		List<CoreMap> sentences = annotation.get(SentencesAnnotation.class);
		
		for(CoreMap sentence: sentences){
			String sentiment = sentence.get(SentimentCoreAnnotations.ClassName.class);
			 
				String s=sentiment+" "+ sentence;
				word = new Text();
				word.set(s.toString());
				 System.out.println(s.toString());
				context.write(word, one);	
	
      
    }
		}	
  }

	 
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			 int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
		    }
		  }
			
		
	
	
	

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	    if (otherArgs.length !=2){
	      System.err.println("Usage: sentiment <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Sentiment");
	    job.setJarByClass(Entity.class);
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

	


