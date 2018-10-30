package com.feicheng.java.flink.office.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Administrator on 2018/9/25.
 */
public class CountWindowAverageMain {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();


        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute("CountWindowAverageMain");

    }
}