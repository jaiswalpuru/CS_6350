import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;

public class InMemoryReduce {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public final Text userID = new Text();
        public final Text fList = new Text();

        public void map(LongWritable key, Text val, Context c) throws IOException, InterruptedException {
            String[] inputValues = val.toString().split("\\t");
            //If no friends are there then ignore
            if (inputValues.length<2) {
                return;
            }
            userID.set(inputValues[0]);
            fList.set(inputValues[1]);
            c.write(userID, fList);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        HashMap<String, Integer> hm = new HashMap<>();

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
            super.setup(c);
            Configuration conf = c.getConfiguration();
            Path fPath = new Path(conf.get("reducer.input"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(fPath);
            for(FileStatus s : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
                String line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        hm.put(arr[0], getAge(arr[9]));
                    line = br.readLine();
                }
            }
        }

        public int getAge(String d) {
            String[] date = d.split("/");
            LocalDate currDate = LocalDate.now();
            LocalDate bDay = LocalDate.of(Integer.parseInt(date[2]),Integer.parseInt(date[0]),Integer.parseInt(date[1]));
            return Period.between(bDay,currDate).getYears();
        }

        public void reduce(Text k, Iterable<Text> v, Context c)throws IOException, InterruptedException {
            for (Text t : v) {
                String[] friends = t.toString().split(",");
                int minAge = Integer.MAX_VALUE;
                for (String f : friends){
                    if (hm.get(f)!= null){
                        minAge = Math.min(minAge, hm.get(f));
                    }
                }
                c.write(k, new Text(Integer.toString(minAge)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("reducer.input", args[1]);
        Job j = new Job(conf, "Minimum Age of Direct friend");
        j.setJarByClass(InMemoryReduce.class);
        j.setMapperClass(InMemoryReduce.Map.class);
        j.setReducerClass(InMemoryReduce.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[2]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
