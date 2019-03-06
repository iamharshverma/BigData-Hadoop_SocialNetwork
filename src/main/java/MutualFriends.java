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
import java.util.LinkedHashSet;

public class MutualFriends {
	/**
	 * @author harshverma
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		Text user = new Text();
		Text friends = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//split the line into user and friends
			String[] split = value.toString().split("\\t");
			//split[0] - user
			//split[1] - friendList
			String userid = split[0];
			if( split.length == 1 ) {
				return;
			}
			String[] others = split[1].split(",");
			for( String friend : others ) {

				if( userid.equals(friend) )
					continue;

				String userKey = (Integer.parseInt(userid) < Integer.parseInt(friend) ) ? userid + "," + friend : friend + "," + userid;
				String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";
				friends.set(split[1].replaceAll(regex, ""));
				user.set(userKey);
				context.write(user, friends);
			}
		}

	}

	public static class Reduce
					extends Reducer<Text,Text,Text,Text> {

		private String findMatchingFriends( String list1, String list2 ) {

			if( list1 == null || list2 == null )
				return null;

			String[] friendsList1 = list1.split(",");
			String[] friendsList2 = list2.split(",");

			//use LinkedHashSet to retain the sort order
			LinkedHashSet<String> set1 = new LinkedHashSet<String>();
			for( String user: friendsList1 ) {
				set1.add(user);
			}

			LinkedHashSet<String> set2 = new LinkedHashSet<String>();
			for( String user: friendsList2 ) {
				set2.add(user);
			}

			//keep only the matching items in set1
			set1.retainAll(set2);

			return set1.toString().replaceAll("\\[|\\]","");
		}

		public void reduce(Text key, Iterable<Text> values,
		                   Context context
		) throws IOException, InterruptedException {

			String[] friendsList = new String[2];
			int i = 0;

			for( Text value: values ) {
				friendsList[i++] = value.toString();
			}
			String mutualFriends = findMatchingFriends(friendsList[0],friendsList[1]);
			if( mutualFriends != null && mutualFriends.length() != 0 ) {
				context.write(key, new Text( mutualFriends ) );
			}
		}

	}
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <FriendsFile> <output>");
			System.exit(2);
		}

// create a job with name "MutualFriends"
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
// set output key type
		job.setOutputKeyClass(Text.class);
// set output value type
		job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
