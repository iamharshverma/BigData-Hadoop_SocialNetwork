import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTopTenFriends {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		final Logger log = LoggerFactory.getLogger(Map.class);

		private Text coupleKey = new Text(); // output key is of text type, holds personA, Person B

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] docLine = value.toString().split("\t");
			if (docLine.length == 2) {
				String[] friendList = docLine[1].split(",");
				for (String data : friendList) {
					String friendPairKey = "";
					if (Integer.parseInt(docLine[0]) > Integer.parseInt(data))
						friendPairKey = data + ", " + docLine[0];
					else
						friendPairKey = docLine[0] + ", " + data;

					coupleKey.set(friendPairKey);
					context.write(coupleKey, new Text(docLine[1]));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// values will be maximum size two , list of (a,b) and (a,b) from (b,a)
			HashSet<String> friendsList1 = new HashSet<String>();
			HashSet<String> friendsList2 = new HashSet<String>();

			int index = 1;
			for (Text val : values) {
				if (index == 1) {
					friendsList1.addAll(Arrays.asList(val.toString().split(",")));

				} else if (index == 2) {
					friendsList2.addAll(Arrays.asList(val.toString().split(",")));
				}
				index++;
			}

			// finding common items of two friends list
			friendsList1.retainAll(friendsList2);

			StringBuilder sb = new StringBuilder();
			for (String s : friendsList1) {
				sb.append(s).append(",");
			}
			if (sb.length() > 0) {
				result.set(sb.substring(0, sb.length() - 1).toString());
				context.write(key, result);
			}
		}
	}

	public static class Map2 extends Mapper<Text, Text, LongWritable, Text> {
		private LongWritable frequency = new LongWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int count = value.toString().split(",").length;
			frequency.set(count);
			context.write(frequency, new Text(key.toString() + "-" + value));
		}
	}

	public static class Reduce2 extends Reducer<LongWritable, Text, Text, Text> {

		private int topTenCount = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
						throws IOException, InterruptedException {
			Text pairValue = new Text();
			for (Text value : values) {
				if (topTenCount < 10) {
					topTenCount++;
					String[] temp = value.toString().split("-");
					pairValue.set(key.toString() + "\t" + temp[1]);
					context.write(new Text(temp[0]), pairValue);
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: MyTopTenFriends <input file path> <output file path1> <output file path2>");
			System.exit(2);
		}

		// job chaining - output of job1 given to input to job2
		// first job - gives pairs and their mutual friends list
		Job getMutualFriendsJob = Job.getInstance(conf1, "MyTopTenFriends");
		getMutualFriendsJob.setJarByClass(MyTopTenFriends.class);
		getMutualFriendsJob.setMapperClass(MyTopTenFriends.Map.class);
		getMutualFriendsJob.setReducerClass(MyTopTenFriends.Reduce.class);
		// set output key type to text
		getMutualFriendsJob.setOutputKeyClass(Text.class);
		// set output value type to text
		getMutualFriendsJob.setOutputValueClass(Text.class);
		// setting hdfs input path for incoming file
		FileInputFormat.addInputPath(getMutualFriendsJob, new Path(otherArgs[0]));
		// setting hdfs output path for storing generated output
		FileOutputFormat.setOutputPath(getMutualFriendsJob, new Path(otherArgs[1]));
		boolean firstMrJobStatus = getMutualFriendsJob.waitForCompletion(true);
		if (!firstMrJobStatus) {
			System.exit(1);
		} else
		// Second Job - gives pairs, count of mutual friends , and mutual friends list
		{
			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2, "MyTopTenFriends");
			job2.setJarByClass(MyTopTenFriends.class);
			job2.setMapperClass(MyTopTenFriends.Map2.class);
			job2.setReducerClass(MyTopTenFriends.Reduce2.class);

			// mapper - output ( key -> count , value - > pairs, mutual friends list )
			// set job2's mapper output key type
			job2.setMapOutputKeyClass(LongWritable.class);
			// set job2's mapper output value type
			job2.setMapOutputValueClass(Text.class);
			// set job2's output key type
			job2.setOutputKeyClass(Text.class);
			// set job2's output value type
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			// sorting map output in descending order
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			// job2.setNumReduceTasks(1);
			// job chaining - 1st job output is 2nd job input
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}
}