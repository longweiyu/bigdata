import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String input=value.toString()
    					  .replace('\"', ' ')
    					  .replace('!', ' ')
    					  .replace('\'', ' ')
    					  .replace('?', ' ')
    					  .replace('(', ' ')
    					  .replace(')', ' ')
    					  .replace('-', ' ')
    					  .replace('.', ' ')
    					  .replace(':', ' ')
    					  .replace(';', ' ')
    					  .replace('=', ' ')
    					  .replace(',',' ')
    					  .replace('[',' ')
    					  .replace(']',' ')
    					  .toLowerCase();
    	
      StringTokenizer itr = new StringTokenizer(input);
     /* while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
     //add all words in a line to a set(thus no duplication)
      Set<String> tokenSet = new HashSet<String>();
        while (itr.hasMoreTokens()) {
            tokenSet.add(itr.nextToken());
        }
     //emit (word,1) for every word in this set.
        for (String val : tokenSet) {
            word.set(val);
            context.write(word, one);
        }
    }
  }

  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    //emit counts for this key from values
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new WordCount(), args);
    }
}