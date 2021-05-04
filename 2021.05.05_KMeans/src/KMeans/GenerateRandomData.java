package KMeans;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Random;

public class GenerateRandomData {
    public static void main(String[] args) {
        Random random = new Random();

        List<List<Double>> centers = new ArrayList<>();
        centers.add(new ArrayList<>(Arrays.asList(10.0, 10.0)));
        centers.add(new ArrayList<>(Arrays.asList(10.0, -10.0)));
        centers.add(new ArrayList<>(Arrays.asList(-10.0, 10.0)));
        centers.add(new ArrayList<>(Arrays.asList(-10.0, -10.0)));

        for (List<Double> center : centers) {
            double cx = center.get(0);
            double cy = center.get(1);
            for (int i = 0; i < 100; i++) {
                double dx = random.nextDouble() * 10 - 5;
                double dy = random.nextDouble() * 10 - 5;
                double x = cx + dx;
                double y = cy + dy;
                System.out.printf("%.2f,%.2f\n", x, y);
            }
        }
    }
}