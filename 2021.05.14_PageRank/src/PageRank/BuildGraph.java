package PageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BuildGraph {
    public static class BuildGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "\t");
            Text fromURL = new Text(stringTokenizer.nextToken());

            Text empty = new Text();
            while (stringTokenizer.hasMoreTokens()) {
                Text toURL = new Text(stringTokenizer.nextToken());
                context.write(fromURL, toURL);
                context.write(toURL, empty);
            }
        }
    }

    public static class BuildGraphReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("1.00");

            int numPages = 0;
            for (Text value : values) {
                if (value.getLength() > 0) {
                    numPages++;
                    stringBuilder.append("\t").append(value);
                }
            }

            if (numPages == 0) stringBuilder.append("\t");
            Text toURLs = new Text(stringBuilder.toString());
            context.write(key, toURLs);
        }
    }
}