import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Aggregation extends Configured implements Tool {

    public static class FloatArrayWritable extends ArrayWritable {
        public FloatArrayWritable() {
            super(FloatWritable.class);
        }
    }

    public static class Map extends Mapper<Object, Text, Text, FloatArrayWritable> {

        private Text userid = new Text();
        private FloatArrayWritable valuesArray = new FloatArrayWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            java.util.Map<String, String> attributes_map = MRDPUtils.transformXmlToMap(value.toString());
            if (attributes_map.containsKey("OwnerUserId")) {
                userid.set(attributes_map.get("OwnerUserId"));
                int userIdNum = Integer.valueOf(userid.toString());
                if (userIdNum > -100 && userIdNum < 100) {
                    String bodyClean = attributes_map.get("Body").replace("&lt", "").replace("p&gt", "").replace("&#xA", "")
                            .replace("code&gt", "").replace("&gt", "").replace("&quot", "").replace(";", "")
                            .replace("/", "");
                    FloatWritable len_title = attributes_map.get("Title") != null
                            ? new FloatWritable(attributes_map.get("Title").length())
                            : new FloatWritable(0.5f);
                    FloatWritable score = attributes_map.get("Score") != null
                            ? new FloatWritable(Float.parseFloat(attributes_map.get("Score")))
                            : new FloatWritable(0.5f);
                    FloatWritable view_count = attributes_map.get("ViewCount") != null ? new FloatWritable(bodyClean.length())
                            : new FloatWritable(0.5f);
                    valuesArray.set(new Writable[]{len_title, score, view_count});
                    context.write(userid, valuesArray);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatArrayWritable, Text, FloatWritable> {

        private Text newKey = new Text();
        private FloatWritable value = new FloatWritable();

        public static float findMean(List<Float> valuesList) {
            int n = valuesList.size();
            float sum = 0;
            for (int i = 0; i < n; i++)
                sum += valuesList.get(i);

            return (sum / n);
        }

        public static void sortValues(List<Float> valuesList) {
            valuesList.sort(null);
        }

        public static float findMedian(List<Float> valuesList) {
            int n = valuesList.size();

            if (n % 2 != 0)
                return valuesList.get(n / 2);

            return ((valuesList.get((n - 1) / 2) + valuesList.get(n / 2)) / 2f);
        }

        public static float findMinimal(List<Float> valuesList) {
            return valuesList.get(0);
        }

        public static float findMaximal(List<Float> valuesList) {
            int n = valuesList.size();

            return valuesList.get(n - 1);
        }

        public static float correlation(List<Float> scoresList, List<Float> titlesList) {
            float sum_X = 0, sum_Y = 0, sum_XY = 0, squareSum_X = 0, squareSum_Y = 0;
            int n = scoresList.size();

            for (int i = 0; i < n; i++) {
                sum_X += scoresList.get(i);
                sum_Y += titlesList.get(i);
                sum_XY = sum_XY + scoresList.get(i) * titlesList.get(i);
                squareSum_X = squareSum_X + scoresList.get(i) * scoresList.get(i);
                squareSum_Y = squareSum_Y + titlesList.get(i) * titlesList.get(i);
            }
            float corr = (n * sum_XY - sum_X * sum_Y) / (float) (Math.sqrt((n * squareSum_X - sum_X * sum_X) * (n * squareSum_Y - sum_Y * sum_Y)));
            if (Float.isNaN(corr)) {
                return 0;
            } else {
                return corr;
            }
        }

        public void reduce(Text key, Iterable<FloatArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            List<Float> viewCountList = new ArrayList<Float>();
            List<Float> scoresList = new ArrayList<Float>();
            List<Float> lenTitlesList = new ArrayList<Float>();

            int count = 0;
            for (ArrayWritable val : values) {
                count++;
                Writable[] info = val.get();
                Float lenTitlesListValue = Float.parseFloat(info[0].toString());
                lenTitlesList.add(lenTitlesListValue);
                Float scoresListValue = Float.parseFloat(info[1].toString());
                scoresList.add(scoresListValue);
                Float viewCountListValue = Float.parseFloat(info[2].toString());
                viewCountList.add(viewCountListValue);
            }
            newKey.set(key.toString() + ":countPosting");
            value.set(count);
            context.write(newKey, value);

            newKey.set(key.toString() + ":average");
            value.set(findMean(lenTitlesList));
            context.write(newKey, value);

            sortValues(lenTitlesList);

            newKey.set(key.toString() + ":median");
            value.set(findMedian(lenTitlesList));
            context.write(newKey, value);

            newKey.set(key.toString() + ":minimal");
            value.set(findMinimal(lenTitlesList));
            context.write(newKey, value);

            newKey.set(key.toString() + ":maximal");
            value.set(findMaximal(lenTitlesList));
            context.write(newKey, value);

            newKey.set(key.toString() + ":corScoreViewCount");
            value.set(correlation(scoresList, viewCountList));
            context.write(newKey, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "aggregation");
        job.setJarByClass(Aggregation.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        Path output = new Path(args[1]);
        output.getFileSystem(conf).delete(output, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Aggregation(), args);
    }
}