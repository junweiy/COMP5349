package task_2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SortingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private Text keyOut = new Text();
    private Map<Text, IntWritable> countMap = new LinkedHashMap<>();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        Iterator<Text> valuesItr = values.iterator();
        while (valuesItr.hasNext()){
            countMap.put(new Text(valuesItr.next().toString()), new IntWritable(key.get()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text key: countMap.keySet()) {
            if (count ++ == 50) {
                break;
            }
            context.write(key, countMap.get(key));
        }
    }
}