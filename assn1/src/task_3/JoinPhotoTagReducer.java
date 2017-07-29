package task_3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The reducer class for the reducer side join for combining the result of task 2 and the last job
 * The output is the top 50 localities alongside with their top 10 tags and frequencies
 * @author Jiamin Dong
 *
 */
public class JoinPhotoTagReducer extends  Reducer<TextIntPair, Text, Text, Text> {

	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		HashMap <String, Integer> tagList = new HashMap<String, Integer>();
		if (key.getOrder().get() == 0){// the key is from the place table
			String numberPhoto = valuesItr.next().toString();
			// Add the tags into a hashmap to count the numbers
			while (valuesItr.hasNext()){
				String tag = valuesItr.next().toString();
				String[] tagSplit = tag.split("\t");
				for(String tagString: tagSplit){
					String[] tagNameCount = tagString.split(",");
					for(String tt: tagNameCount) {
						int index = tt.lastIndexOf(':');
						String tagName = tt.substring(0,index);
						String tagNumber = tt.substring(index+1,tt.length());
						int tagNum = Integer.parseInt(tagNumber);
						if(tagList.containsKey(tagName)){
							int number = tagList.remove(tagName);
							number = number + tagNum;
							tagList.put(tagName, number);
						} else {
							tagList.put(tagName, tagNum);
						}
					}
				}
			}
			// Sort the tags based on frequencies and output the top 10
			HashMap sorted = sortByValues(tagList);
			Set<String> keys = sorted.keySet();
			int num = 0;
			String outputTag = numberPhoto;
			for(String tagt: keys){
				num++;
				outputTag = outputTag + "\t" + tagt + ":"+ tagList.get(tagt);
				if(num == 10){
					break;
				}
			}
			if(outputTag.length() > 0) {
				context.write(key.getKey(), new Text(outputTag));		
			}
		}
	}
	
	// This is the function used to sort the hashmap
	private static HashMap sortByValues(HashMap map) { 
       List list = new LinkedList(map.entrySet());
       // Defined Custom Comparator here
       Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
               return ((Comparable) ((Map.Entry) (o2)).getValue())
                  .compareTo(((Map.Entry) (o1)).getValue());
            }
       });

       // Here I am copying the sorted list in HashMap
       // using LinkedHashMap to preserve the insertion order
       HashMap sortedHashMap = new LinkedHashMap();
       for (Iterator it = list.iterator(); it.hasNext();) {
              Map.Entry entry = (Map.Entry) it.next();
              sortedHashMap.put(entry.getKey(), entry.getValue());
       } 
       return sortedHashMap;
  }

}
