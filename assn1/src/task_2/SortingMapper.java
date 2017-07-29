package task_2;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SortingMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable keyOut = new IntWritable();
    private Text valueOut = new Text();
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        keyOut.set(Integer.parseInt(dataArray[1].toString()));
        valueOut.set(dataArray[0]);
        context.write(keyOut, valueOut);
    }

}
