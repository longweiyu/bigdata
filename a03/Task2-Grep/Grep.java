import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class Grep extends Configured implements Tool {

    private static String searchStr = "The";

    public static class Map
            extends Mapper<Object, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            if(isWordPresent(line[1],searchStr)){
                word.set(line[0]);
                context.write(word,null);
            }
        }
    }

    public static boolean isWordPresent(String sentence,
                                 String word)
    { 
        word = transform(word);
        sentence = transform(sentence);
        String []s = sentence.split(" ");
        for ( String temp :s)
        {
            if (temp.compareTo(word) == 0)
            {
                return true;
            }
        }
        return false;
    }

    public static String transform(String word)
    {
        return word.toUpperCase();
    }
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "grep");
        job.setJarByClass(Grep.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        FileOutputFormat.setOutputPath(job, new Path(args[1]+timeStamp));
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new Grep(), args);
    }
}