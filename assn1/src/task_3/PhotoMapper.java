package task_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * The mapper to read photo
 * 
 * using n0*.txt as the photo table 
 * n0*.txt's format is:
 * photo_id \t owner \t tags \t date_taken\t place_id \t accuracy
 * 
 * output: place_id => tags
 * @author Jiamin Dong
 *
 */
public class PhotoMapper extends Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=5) {
			String tags = dataArray[2];
			String date = dataArray[3];
			String year = date.substring(0,date.indexOf('-'));
			// Check if the tags contain the year of the photo taken, if so, filter the tag out
			if(tags.contains(year)) {
				tags = tags.replace(year, "");
			}
			context.write(new TextIntPair(dataArray[4],1), new Text(tags));
		}
	}

}
