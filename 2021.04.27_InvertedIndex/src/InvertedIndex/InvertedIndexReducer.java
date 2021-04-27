package InvertedIndex;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        boolean isCount = configuration.get("--count") != null;
        String stopwords = configuration.get("stopwords");
        boolean isSkipStopwords = configuration.get("--skip-stopwords") != null;

        if (isSkipStopwords && stopwords.contains("," + key.toString() + ","))
            return;

        Iterator<Text> iter = values.iterator();
        StringBuilder wordsBuilder = new StringBuilder();
        Map<String, Integer> countMap = new HashMap<>();
        while (iter.hasNext()) {
            String filenameOffset = iter.next().toString();
            String filename = filenameOffset.substring(0, filenameOffset.indexOf("@"));
            wordsBuilder.append(filenameOffset).append(";");
            countMap.put(filename, countMap.getOrDefault(filename, 0) + 1);
        }

        if (isCount) {
            StringBuilder filenameBuilder = new StringBuilder();
            for (Map.Entry<String, Integer> e : countMap.entrySet())
                filenameBuilder.append(e.getKey()).append("#").append(e.getValue()).append(";");
            Text value = new Text(filenameBuilder.toString());
            context.write(key, value);
        } else {
            Text value = new Text(wordsBuilder.toString());
            context.write(key, value);
        }
    }
}