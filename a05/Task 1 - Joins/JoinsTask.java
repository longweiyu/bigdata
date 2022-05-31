package A05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.common.base.Strings;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.*;

public class JoinsTask extends Configured implements Tool {
    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

    public static class PostMap extends Mapper<Object, Text, Text, Text> {
        private Text keyUserId = new Text();
        private Text valueScore = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            java.util.Map<String, String> attributes_map = MRDPUtils.transformXmlToMap(value.toString());
            String userid = attributes_map.get("OwnerUserId");
            String score = attributes_map.get("Score");
            if (userid != null && score != null) {
                int uid = Integer.valueOf(userid);
                if (uid >= 20 && uid <= 40) {
                    keyUserId.set(userid);
                    valueScore.set(score);
                    context.write(keyUserId, valueScore);
                    System.out.println("map:key" + keyUserId + " value:" + valueScore);
                }
            }
        }
    }

    public static class UserMap extends Mapper<Object, Text, Text, Text> {
        private Text keyUserId = new Text();
        private Text valueName = new Text();
        private Text valueupVotes = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            java.util.Map<String, String> attributes_map = MRDPUtils.transformXmlToMap(value.toString());
            String userid = attributes_map.get("Id");
            String displayName = attributes_map.get("DisplayName");
            String upVotes = attributes_map.get("UpVotes");
            if (userid != null && !Strings.isNullOrEmpty(displayName) && !Strings.isNullOrEmpty(upVotes)) {
                int uid = Integer.valueOf(userid);
                if (uid >= 20 && uid <= 40) {
                    keyUserId.set(userid);
                    valueName.set("Name:" + displayName);
                    valueupVotes.set("UpVotes:" + upVotes);
                    context.write(keyUserId, valueName);
                    context.write(keyUserId, valueupVotes);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int countPost = 0;
            String valueStr = "";
            String name = "";
            String upvotes = "";
            double sumScore = 0;
            for (Text value : values) {
                valueStr = value.toString();
                System.out.println(valueStr);
                if (valueStr.contains("Name:")) {
                    name = valueStr;
                } else if (valueStr.contains("UpVotes:")) {
                    upvotes = valueStr;
                } else {
                    sumScore += Integer.valueOf(valueStr);
                    countPost++;
                }
            }
            if (countPost != 0) {
                double avgScore = sumScore / countPost;
                newKey.set("id: " + key);
                String stringAll = name + " Posts: " + countPost + String.format(" AVGScore:%.2f ", avgScore) + " " + upvotes;
                newValue.set(stringAll);
                context.write(newKey, newValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "JoinsTask");
        job.setJarByClass(JoinsTask.class);

        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path postInputPath = new Path("data/input/a05/post10000.xml");
        MultipleInputs.addInputPath(job, postInputPath, TextInputFormat.class, PostMap.class);

        Path userInputPath = new Path("data/input/a05/users.xml");
        MultipleInputs.addInputPath(job, userInputPath, TextInputFormat.class, UserMap.class);

        FileOutputFormat.setOutputPath(job, new Path("data/output/a05/" + timeStamp));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new JoinsTask(), args);
    }
}

