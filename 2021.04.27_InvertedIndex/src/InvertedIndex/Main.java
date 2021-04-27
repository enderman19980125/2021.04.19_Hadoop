package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(InvertedIndex.Main.class);
        job.setJobName("InvertedIndex");
        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Configuration configuration = job.getConfiguration();
        configuration.set("stopwords", ",, ,a,b,");
        configuration.set("replace-regex", "([^a-zA-Z']|^'+|'+$)");
        for (String arg : args) {
            if ("--skip-stopwords".equals(arg)) {
                configuration.set("--skip-stopwords", "true");
            }
            if ("--count".equals(arg)) {
                configuration.set("--count", "true");
            }
        }

        job.waitForCompletion(true);
    }
}