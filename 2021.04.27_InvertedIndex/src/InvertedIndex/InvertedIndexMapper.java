package InvertedIndex;

import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String replaceRegex = configuration.get("replace-regex");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        Text filenameOffset = new Text(fileName + "@" + key.toString());

        StringTokenizer iter = new StringTokenizer(value.toString());
        while (iter.hasMoreTokens()) {
            String cleanedWord = iter.nextToken().replaceAll(replaceRegex, "").replaceAll(replaceRegex, "").toLowerCase();
            word.set(cleanedWord);
            context.write(word, filenameOffset);
        }
    }
}