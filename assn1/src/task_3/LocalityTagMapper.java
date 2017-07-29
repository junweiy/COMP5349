package task_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * The mapper to read the output from the previous task
 * 
 * Output: (localityName,1) => tags
 * @author Jiamin Dong
 *
 */
public class LocalityTagMapper extends  Mapper<Object, Text, TextIntPair, Text> {
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		String localityName = dataArray[0];
		String tags = dataArray[1];
		context.write(new TextIntPair(dataArray[0],1), new Text(tags));
	}
}
