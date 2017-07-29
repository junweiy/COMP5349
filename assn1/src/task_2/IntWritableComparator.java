package task_2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;

public class IntWritableComparator extends WritableComparator {
    public IntWritableComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable key1 = (IntWritable) a;
        IntWritable key2 = (IntWritable) b;
        return -1 * new Integer(key1.get()).compareTo(key2.get());
    }
}