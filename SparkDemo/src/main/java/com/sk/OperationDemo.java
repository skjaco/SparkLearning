package com.sk;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 *
 * </p>
 * Email guiyun@uoko.com
 *
 * @author Created by Guiyun on 2019/03/19
 */
public class OperationDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("opera");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<String> testList = Arrays.asList(
                "aaa1", "aaa2", "aaa3", "aaa4",
                "aaa1", "aaa2", "aaa3", "aaa4",
                "aaa1", "aaa2", "aaa3", "aaa4"
        );
        JavaRDD<String> rdd1 = context.parallelize(testList, 3);
        JavaRDD<String> stringJavaRDD = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String word = iterator.next();
                    list.add("index:" + index + "   value:" + word);
                }
                return list.iterator();
            }
        }, true);
        /**
         * repartition:有shuffle算子，可对RDD重新分区
         * coalesce:与repartition一样，但可以指定是否shuffle
         */
//        JavaRDD<String> rdd2 = stringJavaRDD.repartition(4);
        JavaRDD<String> rdd2 = stringJavaRDD.coalesce(4,false);
        JavaRDD<String> stringJavaRDD1 = rdd2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String word = iterator.next();
                    list.add("index:" + index + "   value:" + word);
                }
                return list.iterator();
            }
        }, true);
        List<String> result = stringJavaRDD1.collect();
        for (String s : result){
            System.out.println(s);
        }
        context.stop();
    }
}
