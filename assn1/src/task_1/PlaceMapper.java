package task_1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlaceMapper extends Mapper<Object, Text, Text, Text> {
    private Text placeId = new Text();
    private Text locality = new Text();

    private static int LOCALITY = 7;
    private static int NEIGHBOURHOOD = 22;

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        if (dataArray.length < 7){
            return;
        }

        // don't emit anything when place type is not neighbourhood or locality
        if (Integer.parseInt(dataArray[5]) != NEIGHBOURHOOD && Integer.parseInt(dataArray[5]) != LOCALITY) {
            return;
        }

        // split the place-name by the first comma
        String[] levels = dataArray[4].split(",", 2);
        placeId.set(dataArray[0]);

        // locality name is the same with place-name when place type is locality
        if (Integer.parseInt(dataArray[5]) == LOCALITY) {
            locality.set(dataArray[4]);
        }

        /* locality name is place-name starting from the first comma when place type
           is neighbourhood */
        if (Integer.parseInt(dataArray[5]) == NEIGHBOURHOOD) {
            locality.set(levels[1].trim());
        }
        context.write(placeId, locality);
    }

}
