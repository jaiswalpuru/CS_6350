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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InvertedIndex {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public Text word = new Text();
        public int lineNumber = 1;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split(",");
            for (String str: input) {
                word.set(str);
                context.write(word, new IntWritable(lineNumber));
            }
            lineNumber++;
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> val, Context c) throws IOException, InterruptedException {
            List<Integer> value = new ArrayList<>();
            for(IntWritable i : val){
                value.add(i.get());
            }
            Collections.sort(value);
            c.write(key, new Text(value.toString()));
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
        j.setJarByClass(InvertedIndex.class);
        j.setMapperClass(InvertedIndex.Map.class);
        j.setReducerClass(InvertedIndex.Reduce.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(j, new Path(otherArgs[1]));
        System.out.println(j.waitForCompletion(true) ? 0 : 1);
    }
}
