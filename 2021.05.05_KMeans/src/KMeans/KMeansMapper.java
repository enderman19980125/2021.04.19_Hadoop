package KMeans;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected Map<Integer, double[]> centers;

    protected void setup(Context context) {
        centers = new HashMap<>();
        Configuration configuration = context.getConfiguration();
        int numCenters = Integer.parseInt(configuration.get("num-centers"));

        for (int centerId = 0; centerId < numCenters; centerId++) {
            String keyCenterX = String.format("c%dx", centerId);
            String keyCenterY = String.format("c%dy", centerId);
            double centerX = configuration.getDouble(keyCenterX, 0.0);
            double centerY = configuration.getDouble(keyCenterY, 0.0);
            centers.put(centerId, new double[]{centerX, centerY});
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int indexOfComma = value.toString().indexOf(',');
        double x = Double.parseDouble(value.toString().substring(0, indexOfComma));
        double y = Double.parseDouble(value.toString().substring(indexOfComma + 1));

        int nearestCenterId = -1;
        double nearestDistance = Double.MAX_VALUE;
        for (Map.Entry<Integer, double[]> center : centers.entrySet()) {
            int centerId = center.getKey();
            double centerX = center.getValue()[0];
            double centerY = center.getValue()[1];
            double distance = Math.sqrt(Math.pow(x - centerX, 2) + Math.pow(y - centerY, 2));
            if (nearestCenterId == -1 || distance < nearestDistance) {
                nearestCenterId = centerId;
                nearestDistance = distance;
            }
        }

        Text keyNearestCenter = new Text("c" + nearestCenterId);
        context.write(keyNearestCenter, value);
    }
}