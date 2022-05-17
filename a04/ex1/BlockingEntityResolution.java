import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BlockingEntityResolution extends Configured implements Tool {


    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text identifier = new Text();
        private Text name = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString()
                    //.replaceAll("[0-9]", "").replace('-', ' ').replace('.', ' ').toLowerCase()
                    .split(",");
            if (input.length > 2)
                return;
            for(int i=0;i<input.length;i++){
                String nameStr=input[i];
                String[] name1 = nameStr.trim().split(" ");
                String last_name = name1[name1.length - 1];
                //rule1:last name
                identifier.set("s1_" + last_name);
                name.set(nameStr.trim());
                context.write(identifier, name);
                //rule2:firstname[0]+lastname[0-2]
                System.out.println("---"+nameStr);
                if(last_name.length()>2){
                    identifier.set("s2_" + nameStr.trim().charAt(0) + last_name.trim().substring(0, 3));
                    name.set(nameStr.trim());
                    context.write(identifier, name);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> names = new HashSet<String>();

            for (Text val : values) {
                names.add(val.toString());
            }

            if (names.contains("Renée J. Miller")) {
                for (String unique_tmp : names) {
                    value.set(unique_tmp);
                    context.write(key, value);
                }
            }
        }
    }

    public static String transform(String word)
    {
        return word.toUpperCase();
    }
    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "blockingentityresolution");
        job.setJarByClass(BlockingEntityResolution.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        FileOutputFormat.setOutputPath(job, new Path(args[1]+timeStamp));
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new BlockingEntityResolution(), args);
    }
}