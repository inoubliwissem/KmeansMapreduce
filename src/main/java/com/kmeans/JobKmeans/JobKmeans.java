package com.kmeans.JobKmeans;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobKmeans {
//function to get vector of centroid from file in hdfs 
    /*
    String path : path to hdfs file training set in first step and last result in others
    int nbPoint the number of centroid
    int iteration number of iteration
    
    */
    static Vector getCentroid(String path, int nbPoint, int iteration) {
        Vector v = new Vector();

        Path hdfsPath = new Path(path);
        try {
            Configuration configuration = new Configuration();
          //  configuration.set("fs.defaultFS", "hdfs://localhost:8020");
            FileSystem fs = FileSystem.get(configuration);

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
            String line;
            line = br.readLine();
            int nbe = 1;
            while (line != null && nbe <= nbPoint) {
                //System.out.println(line);
                v.add(line);
                line = br.readLine();
                nbe++;
            }
            if (iteration > 0) {
                fs.delete(new Path(path));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return v;

    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        // k number of cluster
        int k = 3;
        // number of iterations
        int iteration = 0;
        //path to training set
        String pathInput = "hdfs://localhost:8020/train";
        //path of result
        String pathOutput = "hdfs://localhost:8020/rst/kmeansrst";

        for (int i = 0; i < 5; i++) {
            Configuration conf = new Configuration();
            //set k to configure 
            conf.set("nbc", new String("" + k));
            System.out.println("iteration numero" + (i + 1));
            //if iteration= 0, get cendroid from trainingset
            if (i == 0) {
                Vector vc;
                vc = getCentroid(pathInput, k, i);
                //add centroid to config
                for (int j = 0; j < vc.size(); j++) {
                    conf.set("c" + (j + 1), (j + 1) + "," + vc.get(j).toString());
                }
            } 
            //else get centroid from last reult
            else {
                Vector vc;
                vc = getCentroid(pathOutput + (i - 1) + "/part-r-00000", k, i);
                for (int j = 0; j < vc.size(); j++) {
                    String coord[] = vc.get(j).toString().split("\\t");
                    conf.set("c" + (j + 1), (j + 1) + "," + coord[1]);

                }

            }

            Job job = new Job(conf, "Kmenas");

            job.setJarByClass(JobKmeans.class);

            job.setMapperClass(MapKmeans.class);
            job.setReducerClass(ReduceKmeans.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);

            FileInputFormat.addInputPath(job, new Path(pathInput));
            FileOutputFormat.setOutputPath(job, new Path(pathOutput + i));
            
            job.waitForCompletion(true);
        }

    }

}

