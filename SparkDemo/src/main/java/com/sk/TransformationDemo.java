package com.sk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class TransformationDemo {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("Transformation");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> text = context.textFile("D:\\word.txt");
        JavaRDD<String> filter = text.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return !v1.equals("spark storm hive java ");
            }
        });
        /**
         * Sample算子
         * arg1:有无放回抽样
         * arg2:抽样比例
         * arg3:seed，针对同一批数据只要seed相同，抽样数据相同
         */
//        JavaRDD<String> result = text.sample(true, 0.1);
        filter.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        context.stop();
    }
}
