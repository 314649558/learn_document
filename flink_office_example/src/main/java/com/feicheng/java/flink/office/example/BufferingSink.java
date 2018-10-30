package com.feicheng.java.flink.office.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuanhailong on 2018/9/26.
 */
public class BufferingSink implements SinkFunction<Tuple2<String,Integer>> ,CheckpointedFunction{

    private final int threshold;

    private transient ListState<Tuple2<String,Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;


    public BufferingSink(int threshold){
        this.threshold=threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        if(bufferedElements.size()==this.threshold){
            for(Tuple2<String,Integer> element:bufferedElements){
                //TODO 发送到sink
            }
        }
    }

    @Override
    //只要发生快照就会执行该方法
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //快照发生的时候清空之前的快照信息，添加现有的数据到快照
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    //初始化，快照恢复的时候回执行该方法
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<Tuple2<String, Integer>>(
                        "buffered-elements",//名称必须全局唯一
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);


        if(context.isRestored()){
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
