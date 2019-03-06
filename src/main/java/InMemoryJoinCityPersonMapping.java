import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
/**
 * @author harshverma
 */
public class InMemoryJoinCityPersonMapping extends Configured implements Tool {
	static HashMap<String, String> userData;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text list = new Text();
		private Text userid = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			userData = new HashMap<String, String>();
			String userDataPath =config.get("userdata");
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("hdfs://"+userDataPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					String data = arr[1] + ":" + arr[4];
					userData.put(arr[0].trim(), data);
				}
				line = br.readLine();
			}

			String[] user = value.toString().split("\t");
			if ((user.length == 2)) {

				String[] friend_list = user[1].split(",");
				int n = friend_list.length;
				StringBuilder res=new StringBuilder("[");
				for (int i = 0; i < friend_list.length; i++) {
					if(userData.containsKey(friend_list[i]))
					{
						if(i==(friend_list.length-1))
							res.append(userData.get(friend_list[i]));
						else
						{
							res.append(userData.get(friend_list[i]));
							res.append(",");
						}
					}
				}
				res.append("]");
				userid.set(user[0]);
				list.set(res.toString());
				context.write(userid, list);
			}

		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new InMemoryJoinCityPersonMapping(), args);
		System.exit(res);

	}


	public int run(String[] otherArgs) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 6) {
			System.err.println("Usage: InMemoryJoinCityPersonMapping <userA> <userB> <in> <out> <userdata> <userout>");
			System.exit(2);
		}
		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);
		// create a job with name "InMemoryJoinCityPersonMapping"
		Job job = new Job(conf, "InlineArgument");
		job.setJarByClass(InMemoryJoinCityPersonMapping.class);
		job.setMapperClass(MutualFriendsUserEnd.Map.class);
		job.setReducerClass(MutualFriendsUserEnd.Reduce.class);
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
		Path p=new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);
		// Wait till job completion
		int code = job.waitForCompletion(true) ? 0 : 1;

		Configuration conf1 = getConf();
		conf1.set("userdata", otherArgs[4]);
		Job job2 = new Job(conf1, "InMemoryJoinCityPersonMapping");
		job2.setJarByClass(InMemoryJoinCityPersonMapping.class);

		job2.setMapperClass(Map.class);
		//job2.setReducerClass(Reduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2,p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

		// Execute job and grab exit code
		code = job2.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		return code;

	}

}