package KMeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    private static int iter = 0;
    private static int numCenters;
    private static String inputPath;
    private static String outputPath;
    private static final List<List<double[]>> centersList = new LinkedList<>();

    private static void parseArgs(String[] args) throws ArrayIndexOutOfBoundsException {
        inputPath = args[0];
        outputPath = args[1];

        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            if ("--num-centers".equals(arg)) {
                numCenters = Integer.parseInt(args[++i]);
            } else {
                throw new ArrayIndexOutOfBoundsException(String.format("The argument \"%s\" is invalid.", arg));
            }
        }
    }

    private static void initializeCenters() {
        List<double[]> centers = new ArrayList<>(numCenters);
        Random random = new Random();

        for (int centerId = 0; centerId < numCenters; centerId++) {
            double centerX = 10 * random.nextDouble();
            double centerY = 10 * random.nextDouble();
            centers.add(new double[]{centerX, centerY});
        }

        centersList.add(centers);
    }

    private static void getCentersFromOutputPath() throws FileNotFoundException {
        List<double[]> centers = new ArrayList<>(numCenters);

        String filename = String.format("output-%d/part-r-00000", iter);
        File file = new File(filename);
        Scanner scanner = new Scanner(file);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            int indexOfTab = line.indexOf('\t');
            int indexOfComma = line.indexOf(',');
            double centerX = Double.parseDouble(line.substring(indexOfTab, indexOfComma));
            double centerY = Double.parseDouble(line.substring(indexOfComma + 1));
            centers.add(new double[]{centerX, centerY});
        }

        scanner.close();
        centersList.add(centers);
    }

    private static Job getJobInstance() throws IOException {
        Job job = Job.getInstance();
        job.setJobName("KMeans");
        job.setJarByClass(KMeans.Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(KMeans.KMeansMapper.class);
        job.setReducerClass(KMeans.KMeansReducer.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "-" + iter));

        Configuration configuration = job.getConfiguration();
        configuration.set("num-centers", Integer.toString(numCenters));

        for (int centerId = 0; centerId < numCenters; centerId++) {
            double[] center = centersList.get(iter - 1).get(centerId);

            String keyCenterX = String.format("c%dx", centerId);
            configuration.setDouble(keyCenterX, center[0]);

            String keyCenterY = String.format("c%dy", centerId);
            configuration.setDouble(keyCenterY, center[1]);
        }

        return job;
    }

    private static boolean hasCentersChanged() {
        if (iter == 0) return true;

        for (int centerId = 0; centerId < numCenters; centerId++) {
            double[] oldCenter = centersList.get(iter - 1).get(centerId);
            double[] currentCenter = centersList.get(iter).get(centerId);
            if (Math.abs(currentCenter[0] - oldCenter[0]) > 1e-2 || Math.abs(currentCenter[1] - oldCenter[1]) > 1e-2)
                return true;
        }

        return false;
    }

    private static void printCentersTable() {
        System.out.print("iter");
        for (int k = 0; k < numCenters; k++) System.out.printf(",c%dx,c%dy", k, k);
        System.out.println();

        for (int i = 0; i <= iter; i++) {
            System.out.print(i);
            for (double[] center : centersList.get(i)) {
                System.out.printf(",%.2f,%.2f", center[0], center[1]);
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        parseArgs(args);
        initializeCenters();

        while (hasCentersChanged()) {
            iter++;
            Job job = getJobInstance();
            job.waitForCompletion(true);
            getCentersFromOutputPath();
        }

        printCentersTable();
    }
}