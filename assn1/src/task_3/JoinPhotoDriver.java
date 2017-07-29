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

public class JoinPhotoDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "join photo tag");
		job.setJarByClass(JoinPhotoDriver.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), 
				TextInputFormat.class,LocalityPhotoMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,LocalityTagMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.setMapOutputKeyClass(TextIntPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		job.setReducerClass(JoinPhotoTagReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		job.waitForCompletion(true);
	}
}
