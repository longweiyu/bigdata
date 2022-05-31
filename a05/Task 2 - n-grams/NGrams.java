package A05;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

public class NGrams extends Configured implements Tool {
    public String inputFile = "data/input/a05/corpus.txt";
    public String outputFile = "data/output/a05/";
    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString().replace('\"', ' ').replace('!', ' ').replace('\'', ' ').replace('?', ' ')
                    .replace('(', ' ').replace(')', ' ').replace('-', ' ').replace('.', ' ').replace(':', ' ')
                    .replace(';', ' ').replace('=', ' ').replace(',', ' ').replace('[', ' ').replace(']', ' ')
                    .toLowerCase();
            StringTokenizer itr = new StringTokenizer(input);
            String n2 = "#";
            String n3 = "#";
            String n4 = "#";
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                n2 = n2 + " " + temp;
                context.write(new Text(n2), one);
                if (n3.split(" ").length > 1) {
                    n3 = n3 + " " + temp;
                    context.write(new Text(n3), one);
                }
                if (n4.split(" ").length > 2) {
                    n4 = n4 + " " + temp;
                    context.write(new Text(n4), one);
                }
                n4 = n3;
                n3 = n2;
                n2 = temp;
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > 1000) {
                result.set(sum);
                String keyStr = String.format("%-30s" , key.toString());
                context.write(new Text(keyStr), result);
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "NGrams");
        job.setJarByClass(NGrams.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile + timeStamp));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new NGrams(), args);
    }
}