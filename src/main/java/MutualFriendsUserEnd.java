import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashSet;

public class MutualFriendsUserEnd {
	static String user1 = "";
	static String user2 = "";
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text userid = new Text();
		private Text list = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			user1=config.get("userA");
			user2 = config.get("userB");
			String[] user = value.toString().split("\t");
			if ((user.length == 2)&&(user[0].equals(user1)||user[0].equals(user2))) {

				String[] friend_list = user[1].split(",");
				list.set(user[1]);
				for (int i = 0; i < friend_list.length; i++) {
					String map_key;
					if (Integer.parseInt(user[0]) < Integer.parseInt(friend_list[i])) {
						map_key = user[0] + "," + friend_list[i];
					} else {
						map_key = friend_list[i] + "," + user[0];
					}
					userid.set(map_key);
					context.write(userid, list);
				}


			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet hs = new HashSet();
			int i = 0;

			String res ="";
			for (Text value : values) {
				String[] val = value.toString().split(",");
				for (int j = 0; j < val.length; j++) {
					if (i == 0) {
						hs.add(val[j]);
					} else {
						if (hs.contains(val[j])) {
							res=res.concat(val[j]);
							res=res.concat(",");
							hs.remove(val[j]);
						}

					}
				}
				i++;
			}
			if(!res.equals("")){
				result.set(res);
				context.write(key, result);
			}
		}

	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: MutualFriends <userA> <userB> <in> <out>");
			System.exit(2);
		}
		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);
		// create a job with name "MutualFriends"
		Job job = new Job(conf, "MutualFriendsUserEnd");
		job.setJarByClass(MutualFriendsUserEnd.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// job.setInputFormatClass(Text.class);
		// uncomment the following line to add the Combiner
		// job.setcombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}