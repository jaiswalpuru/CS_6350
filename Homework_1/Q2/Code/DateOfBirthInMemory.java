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
import java.util.*;

public class DateOfBirthInMemory {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public final Text user = new Text();
        public final Text fList = new Text();
        //To do the preprocessing and store it in HashMap
        HashMap<String,String> hm = new HashMap<>();

        @Override
        public void setup(Context c) throws IOException, InterruptedException {
            super.setup(c);
            Configuration conf = c.getConfiguration();
            Path fPath = new Path(conf.get("mapper.input"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(fPath);
            for(FileStatus s : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
                String line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        hm.put(arr[0], arr[9]);
                    line = br.readLine();
                }
            }
        }

        public void map(LongWritable key, Text val, Context c) throws IOException, InterruptedException {
            String[] inputValues = val.toString().split("\\t");
            //If no friends are there then ignore
            if (inputValues.length<2) {
                return;
            }
            String userID = inputValues[0];
            String[] friends = inputValues[1].split(",");
            String s = "";
            for (String f : friends)  s += hm.get(f) + ",";
            s.substring(0,s.length()-1);
            for (String f : friends) {
                if (userID.equals(f)) {
                    continue;
                }
                String userKey = (userID.compareTo(f) < 0) ?
                        userID + "," + f : f + "," + userID;

                fList.set(s);
                user.set(userKey);
                c.write(user, fList);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public static String userOne="", userTwo="";

        @Override
        public void setup(Context c){
            Configuration conf = c.getConfiguration();
            userOne = conf.get("UserOne");
            userTwo = conf.get("UserTwo");
        }

        public String getMutualFriends(String l1, String l2) {
            if (l1 == null || l2 == null){
                return null;
            }

            Set<String> f_set1 = new HashSet<>(Arrays.asList(l1.split(",")));
            Set<String> f_set2 = new HashSet<>(Arrays.asList(l2.split(",")));
            f_set1.retainAll(f_set2);
            return f_set1.toString();
        }

        public void reduce(Text k, Iterable<Text> v, Context c)throws IOException, InterruptedException {
            List<String> l = new ArrayList<>();
            for (Text t : v) l.add(t.toString());
            if (l.size() == 1) l.add("");
            String mf = getMutualFriends(l.get(0), l.get(1));
            String[] mfSplit = mf.split(",");
            if (mfSplit.length > 1) {
                StringBuilder sb = new StringBuilder(mfSplit[0]);
                sb.deleteCharAt(0);
                mfSplit[0] = sb.toString();
                sb = new StringBuilder(mfSplit[mfSplit.length - 1]);
                sb.deleteCharAt(sb.length() - 1);
                mfSplit[mfSplit.length - 1] = sb.toString();
            }
            String mfs = "[";
            Integer ageAfter1995 = 0;
            for (String f : mfSplit) {
                if (f.charAt(0) != '[') {
                    String date = f.substring(f.length() - 4);
                    if (date.compareTo("1995") > 0) {
                        ageAfter1995++;
                    }
                    mfs += f + ", ";
                }
            }
            mfs += "] " + ageAfter1995;
            String t = userOne+","+userTwo;
            if (t.compareTo(k.toString())==0){
                c.write(k, new Text(mfs));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapper.input", args[1]);
        conf.set("UserOne", args[2]);
        conf.set("UserTwo", args[3]);
        Job j = new Job(conf, "Mutual Friends In Memory Join");
        j.setJarByClass(DateOfBirthInMemory.class);
        j.setMapperClass(DateOfBirthInMemory.Map.class);
        j.setReducerClass(DateOfBirthInMemory.Reduce.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[4]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
