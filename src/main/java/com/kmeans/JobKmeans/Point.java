/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kmeans.JobKmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author inb
 */
public class Point implements WritableComparable<Point> {

    DoubleWritable x, y;
    IntWritable idp;

    Point(DoubleWritable x, DoubleWritable y, IntWritable id) {
        this.x = x;
        this.y = y;
        this.idp = id;
    }

    Point() {
        this.x = new DoubleWritable();
        this.y = new DoubleWritable();
        this.idp = new IntWritable();
    }

    public IntWritable getIdp() {
        return idp;
    }

    public void setIdp(IntWritable idp) {
        this.idp = idp;
    }

    public DoubleWritable getX() {
        return x;
    }

    public void setX(DoubleWritable x) {
        this.x = x;
    }

    public DoubleWritable getY() {
        return y;
    }

    public void setY(DoubleWritable y) {
        this.y = y;
    }

    public double getdistance(Point p) {

        return Math.sqrt(Math.pow((p.getY().get() - this.getY().get()), 2) + Math.pow((p.getX().get() - this.getX().get()), 2));

    }

    @Override
    public String toString() {
        return idp +","+x + "," + y;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        x.write(d);
        y.write(d);
        idp.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.x.readFields(di);
        this.y.readFields(di);
        this.idp.readFields(di);
    }

    @Override
    public int compareTo(Point o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
