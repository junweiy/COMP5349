package task_1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The reducer class for the reducer side join
 * it joins test data n05.txt and place.txt
 * @author zhouy
 *
 */
public class ReplicateJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable valueOut = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> valuesItr = values.iterator();
        Integer sum = 0;
        while (valuesItr.hasNext()){
            sum += valuesItr.next().get();
        }
        valueOut.set(sum);
        context.write(key, valueOut);
    }
}
