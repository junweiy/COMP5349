package task_3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The reducer class for the reducer side join
 * it joins test data n0*.txt and place.txt
 * The output would be the locality name and tags with frequencies
 * @author Jiamin Dong
 *
 */
public class JoinReducer extends  Reducer<TextIntPair, Text, Text, Text> {

	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		Hashtable <String, Integer> tagList = new Hashtable<String, Integer>();
		if (key.getOrder().get() == 0){// the key is from the place table
			String localityName = valuesItr.next().toString();
			// Add the number of tags occurrence into the hashmap
			while (valuesItr.hasNext()){
				String tag = valuesItr.next().toString();
				String[] tagSplit = tag.split(" ");
				for(String tt: tagSplit){
					if(!localityName.toLowerCase().contains(tt.toLowerCase())) {
						if(tagList.containsKey(tt)){
							int number = tagList.remove(tt);
							number = number + 1;
							tagList.put(tt, number);
						} else {
							tagList.put(tt, 1);
						}
					}
				}
			}
			Set<String> keys = tagList.keySet();
			String outputTag = "";
			for(String tagt: keys){
				outputTag = outputTag + tagt + ":"+ tagList.get(tagt) + ",";
			}
			if(outputTag.length() > 0) {
				context.write(new Text(localityName), new Text(outputTag));		
			}
		}
	}
}
