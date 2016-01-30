
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class wordcount {
	
	
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		String regex = "[.,\"!--;:?'\\]]"; 
		Text word = new Text();
		final static IntWritable one = new IntWritable(1);
		
	
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String s = null;
			String line = value.toString().toLowerCase();
			line = line.replaceAll(regex, " "); 
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				s = tokenizer.nextToken();
				word.set(s);
				context.write(word, one);
							
			}
		}
	}
	
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)	
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        
	      public int compare(WritableComparable a, WritableComparable b) {
	    	  return -super.compare(a, b);
	      }
	      
	      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	          return -super.compare(b1, s1, l1, b2, s2, l2);
	      }
	}
	
	public static void main(String[] args){
		boolean exit = false;
		String tempDir = "wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
		Configuration conf = new Configuration();
		try{
			Job job = Job.getInstance(conf, "wordcountjob-1");
			job.setJarByClass(wordcount.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setMapperClass(WordCountMap.class);     
		    job.setReducerClass(WordCountReduce.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(tempDir));
		    
		    if(job.waitForCompletion(true)){		    
				Job job2 = Job.getInstance(conf, "wordcountjob-2");
				job2.setJarByClass(wordcount.class);
				job2.setInputFormatClass(SequenceFileInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);		
				job2.setOutputKeyClass(IntWritable.class);
			    job2.setOutputValueClass(Text.class);
			    job2.setMapperClass(InverseMapper.class);    
			    job2.setNumReduceTasks(1); 
			    FileInputFormat.addInputPath(job2, new Path(tempDir));
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			    
			    job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
			    exit = job2.waitForCompletion(true);
		    }
		}catch(Exception e){
			e.printStackTrace();
		}
	}
         
 }
