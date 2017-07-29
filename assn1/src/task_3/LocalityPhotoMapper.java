package task_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * The mapper to the output from task 2
 * output: (localityName,0) => numberPhoto
 * @author Jiamin Dong
 *
 */
public class LocalityPhotoMapper extends Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		String localityName = dataArray[0];
		String numberPhoto = dataArray[1];
		context.write(new TextIntPair(localityName,0), new Text(numberPhoto));
	}

}
