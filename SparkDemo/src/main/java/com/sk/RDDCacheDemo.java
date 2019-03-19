package com.sk;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

public class RDDCacheDemo {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("Cache");
        SparkContext context = new SparkContext(conf);
        RDD<String> text = context.textFile("D:\\word.txt", 2);

        context.stop();
    }
}
