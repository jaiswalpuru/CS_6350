/*
* Author := Puru Jaiswal
*/

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
import java.util.*;

public class MutualFriends {

    public static class Map extends Mapper <LongWritable, Text, Text, Text> {
        private final Text user = new Text();
        private final Text friendList = new Text();

        public Boolean checkFriendsOutput(String key){
            return key.compareTo("0, 1")==0 || key.compareTo("20, 28193")==0 || key.compareTo("1, 29826")==0 ||
                    key.compareTo("6222, 19272")==0 || key.compareTo("28041, 28056")==0;
        }

        protected void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] in = val.toString().split("\\t");
            System.out.println("Input list length : " + in.length);
            //if the input list contains a single person
            if (in.length < 2) {
                return;
            }
            //extract the userID and its friends from the list
            Long  userID = Long.parseLong(in[0]);
            String[] friends = in[1].toString().split(",");
            for (String friend : friends) {

                //avoid loop with the same person
                if (userID == Long.parseLong(friend)){
                    continue;
                }

                String uKey = (userID < Long.parseLong(friend))? userID + ", " +friend : friend + ", " + userID;
                if (checkFriendsOutput(uKey)){
                    user.set(uKey);
                    friendList.set(in[1]);
                    context.write(user, friendList);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        HashMap<String, String> friendsPair = new HashMap<>();

        public String getMutualFriends(String l1, String l2) {
            if (l1 == null || l2 == null) {
                return null;
            }

            Set<String> s1 = new HashSet<String>(Arrays.asList(l1.split(",")));
            Set<String> s2 = new HashSet<String>(Arrays.asList(l2.split(",")));

            s1.retainAll(s2);
            return s1.toString();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();

            for (Text val : values){
                list.add(val.toString());
            }

            if (list.size()==1) {
                list.add("");
            }

            String f1=list.get(0),f2 = list.get(1);

            String mutualFriends = getMutualFriends(f1, f2);
            if (mutualFriends != null) {
                context.write(key, new Text(mutualFriends));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] cmdArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (cmdArgs.length != 2){
            System.out.println("Number of arguments supplied is not correct");
            System.exit(2);
        }

        //create a job with name MutualFriends
        Job job = new Job(conf, "MutualFriends");

        //set jar for MutualFriends
        job.setJarByClass(MutualFriends.class);

        job.setMapperClass(MutualFriends.Map.class);
        job.setReducerClass(MutualFriends.Reduce.class);

        //set output key type
        job.setOutputKeyClass(Text.class);

        //set output value type
        job.setOutputValueClass(Text.class);

        //set hdfs path for input data
        FileInputFormat.addInputPath(job, new Path(cmdArgs[0]));
        System.out.println("Command line args  :" + cmdArgs[0]);
        //set hdfs path for output data
        FileOutputFormat.setOutputPath(job, new Path(cmdArgs[1]));
        System.out.println("Command line args  :" + cmdArgs[1]);
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
