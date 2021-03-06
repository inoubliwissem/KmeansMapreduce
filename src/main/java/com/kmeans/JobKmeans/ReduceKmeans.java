/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kmeans.JobKmeans;

import java.io.IOException;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceKmeans extends Reducer<IntWritable, Point, IntWritable, Point> {

    private Point point = new Point();
    private DoubleWritable abs = new DoubleWritable();
    private DoubleWritable ord = new DoubleWritable();
    private Vector<Point> clusters = new Vector<>();

    protected void setup(Mapper.Context context)
            throws IOException,
            InterruptedException {
        Configuration config = context.getConfiguration();
        int nbc = Integer.parseInt(config.get("nbc"));
        for (int i = 1; i <= nbc; i++) {

            String line[] = config.get("c" + i).split(",");
            Point p = new Point(new DoubleWritable(Double.parseDouble(line[1])), new DoubleWritable(Double.parseDouble(line[2])), new IntWritable(Integer.parseInt(line[0])));
            clusters.add(p);

        }
    }

    public void reduce(IntWritable key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        double x = 0;
        double y = 0;
        int nb = 0;
        for (Point val : values) {
            x += val.getX().get();
            y += val.getY().get();
            nb++;

        }
        x = x / nb;
        y = y / nb;
        Point c = new Point(new DoubleWritable(x), new DoubleWritable(y), key);
//        for (int i = 0; i < clusters.size(); i++) {
//            if (c.getIdp().get() == clusters.get(i).getIdp().get()) {
//                clusters.set(i, c);
//            }
//        }
//        for (int i = 0; i < clusters.size(); i++) {
//            context.write(clusters.get(i).getIdp(), clusters.get(i));
//        }
        context.write(key, c);
//        context.write(key, c);
    }
}
