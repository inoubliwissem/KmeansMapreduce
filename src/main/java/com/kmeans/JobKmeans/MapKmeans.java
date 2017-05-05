/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kmeans.JobKmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class MapKmeans extends Mapper<LongWritable, Text, IntWritable, Point> {

    private static final IntWritable one = new IntWritable(1);

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

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        abs.set(Double.parseDouble(words[0]));
        ord.set(Double.parseDouble(words[1]));

        point.setX(abs);
        point.setY(ord);
        //var
        double minDis = Double.MAX_VALUE;
        int posMin = 0;
        for (int i = 0; i < clusters.size(); i++) {
            double d = point.getdistance(clusters.get(i));
            if (d < minDis) {
                minDis = d;
                Collections.shuffle(clusters);
                posMin = clusters.get(i).getIdp().get();
            }

        }

        context.write(new IntWritable(posMin), point);
    }
}
