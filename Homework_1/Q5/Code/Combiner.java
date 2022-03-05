import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Combiner {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        public static IntWritable wordLength = new IntWritable(-1);

        public void map(LongWritable key, Text val, Context c) throws IOException, InterruptedException {
            String[] str = val.toString().split("\\t");
            word.set(str[0]);
            StringBuilder sb = new StringBuilder(str[1]);
            sb.deleteCharAt(0);
            sb.deleteCharAt(sb.length()-1);
            wordLength.set(sb.toString().split(",").length);
            c.write(word, wordLength);
        }
    }

    public static class CombineReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable cnt = new IntWritable(0);
        Integer maxOccurrences = 0;
        public void reduce(Text key, Iterable<IntWritable> val, Context c) throws IOException, InterruptedException {
            for (IntWritable i : val) {
                if (maxOccurrences < i.get()){
                    maxOccurrences = i.get();
                }
                if (maxOccurrences == i.get()){
                    c.write(key, new IntWritable(maxOccurrences));
                }
            }
        }
    }

    public static class Reduce extends Reducer <Text, IntWritable, Text, IntWritable> {

        private IntWritable cnt = new IntWritable(0);
        Integer maxOccurrences = 0;
        String maxOccurString = "";
        public void reduce(Text key, Iterable<IntWritable> val, Context c) throws IOException, InterruptedException {
            for (IntWritable i : val) {
                if (maxOccurrences < i.get()){
                    maxOccurrences = i.get();
                    maxOccurString = key.toString();
                }
            }
        }

        @Override
        public void cleanup(Context c)throws IOException, InterruptedException {
            c.write(new Text(maxOccurString), new IntWritable(maxOccurrences));
            super.cleanup(c);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //get all args
        if (otherArgs.length != 2) {
            System.out.println(otherArgs.length);
            System.err.println("Usage: Inverted Index <in> <out>");
            System.exit(2);
        }

        Job j = new Job(conf, "Inverted Index");
        j.setJarByClass(Combiner.class);
        j.setMapperClass(Combiner.Map.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setCombinerClass(Combiner.CombineReduce.class);
        j.setReducerClass(Combiner.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(j, new Path(otherArgs[1]));
        System.out.println(j.waitForCompletion(true) ? 0 : 1);
    }
}
