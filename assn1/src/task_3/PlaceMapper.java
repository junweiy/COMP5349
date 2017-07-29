package task_3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * The mapper to read place data
 * 
 * For testing, using place.txt as the photo table 
 * place.txt has the format:
 * place_id \t woeid \t longitude \t latitude \t place name \t place_type_id \t place_url
 * 
 * With localityName, we only care about place_type_id is 7 or 22
 * Output: place_id => localityName
 * @author Jiamin Dong
 *
 */
public class PlaceMapper extends  Mapper<Object, Text, TextIntPair, Text> {
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >=6) {
			String typeId = dataArray[5];
			String localityName = dataArray[4];
			if (typeId.equals("7")) {
				context.write(new TextIntPair(dataArray[0],0), new Text(localityName));
			// Extract the localityName from the full place name
			} else if(typeId.equals("22")) {
				int length = localityName.length();
				int index = localityName.indexOf(',');
				localityName = localityName.substring(index+2, length);
				context.write(new TextIntPair(dataArray[0],0), new Text(localityName));
			}
		}
	}
}
