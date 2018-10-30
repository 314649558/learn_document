package com.feicheng.java.flink.office.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Created by yuanhailong on 2018/9/25.
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {
    /**
     * ValueState 句柄  第一个元素是数量 第二个元素是求和。
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        //访问 state
        Tuple2<Long, Long> currentSum = sum.value();

        // 更新数量
        currentSum.f0 += 1;

        //第二个元素求和
        currentSum.f1 += input.f1;

        //更新状态
        sum.update(currentSum);

        //如果状态达到2 ，发送平均值并清空
        if(currentSum.f0>=2){
            out.collect(new Tuple2<>(input.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long,Long>> descriptor=
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),Tuple2.of(0L,0L));


        StateTtlConfig ttlConfig=StateTtlConfig.
                newBuilder(Time.seconds(1)).
                setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).
                setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).
                build();


        descriptor.enableTimeToLive(ttlConfig);


        sum=getRuntimeContext().getState(descriptor);
    }
}
