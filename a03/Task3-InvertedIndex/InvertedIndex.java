package chap03;

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
import org.apache.hadoop.shaded.com.google.common.base.Joiner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class InvertedIndex extends Configured implements Tool {

    public static class Map
            extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable docid = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            String input = line[1].toString().replace('\"', ' ')
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
            System.out.println(value.toString());
            System.out.println(input);
            StringTokenizer itr = new StringTokenizer(input);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                docid.set(Integer.parseInt(line[0]));
                System.out.println(word+","+docid);
                context.write(word, docid);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,IntWritable,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Set<Integer> ids = new HashSet<Integer>();
            System.out.println(ids);
            for (IntWritable val : values) {
                ids.add(Integer.valueOf(val.toString()));
            }
            if (ids.size() > 3) {
                List<Integer> orderIds = ids.stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());
                result.set(Joiner.on(",").join(orderIds));
                context.write(key, result);
            }
        }
    }
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        FileOutputFormat.setOutputPath(job, new Path(args[1]+timeStamp));
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new InvertedIndex(), args);
    }
}