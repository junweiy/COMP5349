package task_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf, "join place photo");
		job.setJarByClass(JoinDriver.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), 
				TextInputFormat.class,PhotoMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,PlaceMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.setMapOutputKeyClass(TextIntPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
