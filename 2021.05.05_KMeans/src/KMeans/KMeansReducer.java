package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        int centerId = Integer.parseInt(key.toString().substring(1));
        String keyCenterX = String.format("c%dx", centerId);
        String keyCenterY = String.format("c%dy", centerId);
        double oldCenterX = configuration.getDouble(keyCenterX, 0.0);
        double oldCenterY = configuration.getDouble(keyCenterY, 0.0);

        double sumX = 0.0;
        double sumY = 0.0;
        int nPoints = 0;

        for (Text value : values) {
            String point = value.toString();
            int indexOfComma = point.indexOf(',');
            double x = Double.parseDouble(point.substring(0, indexOfComma));
            double y = Double.parseDouble(point.substring(indexOfComma + 1));
            sumX += x;
            sumY += y;
            nPoints++;
        }

        double currentCenterX = oldCenterX;
        double currentCenterY = oldCenterY;

        if (nPoints > 0) {
            currentCenterX = sumX / nPoints;
            currentCenterY = sumY / nPoints;
        }

        Text currentCenter = new Text(String.format("%.2f,%.2f", currentCenterX, currentCenterY));
        context.write(key, currentCenter);
    }
}