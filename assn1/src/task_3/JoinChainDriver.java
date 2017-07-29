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

public class JoinChainDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: JoinDriver <in1> <in2> <in3> <out>");
			System.exit(2);
		}
		Path tmpFilterOut = new Path("tmpFilterOuttask45j3");
		Job job = new Job(conf, "join place photo");
		job.setNumReduceTasks(10); 
		job.setJarByClass(JoinDriver.class);
		
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);
		MultipleInputs.addInputPath(job, photoTable, 
				TextInputFormat.class,PhotoMapper.class);
		MultipleInputs.addInputPath(job, placeTable, TextInputFormat.class,PlaceMapper.class);
		FileOutputFormat.setOutputPath(job, tmpFilterOut);
		job.setMapOutputKeyClass(TextIntPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		job.waitForCompletion(true);
		
		
		Job tagjob = new Job(conf, "join photo tag");
		tagjob.setNumReduceTasks(1); 
		tagjob.setJarByClass(JoinPhotoDriver.class);
		Path photoNumberTable = new Path(otherArgs[2]);
		Path tagTable = new Path("tmpFilterOuttask45/part-r-00000");
		Path outputTable = new Path(otherArgs[3]);
		MultipleInputs.addInputPath(tagjob, photoNumberTable, 
				TextInputFormat.class,LocalityPhotoMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00000"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00001"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00002"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00003"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00004"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00005"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00006"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00007"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00008"), TextInputFormat.class,LocalityTagMapper.class);
		MultipleInputs.addInputPath(tagjob, new Path("tmpFilterOuttask45j3/part-r-00009"), TextInputFormat.class,LocalityTagMapper.class);
		FileOutputFormat.setOutputPath(tagjob, outputTable);
		tagjob.setMapOutputKeyClass(TextIntPair.class);
		tagjob.setMapOutputValueClass(Text.class);
		tagjob.setOutputKeyClass(Text.class);
		tagjob.setOutputValueClass(Text.class);
		tagjob.setGroupingComparatorClass(JoinGroupComparator.class);
		tagjob.setReducerClass(JoinPhotoTagReducer.class);
		tagjob.setPartitionerClass(JoinPartitioner.class);
		System.exit(tagjob.waitForCompletion(true) ? 0 : 1);
	}
}
