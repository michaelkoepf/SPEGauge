package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.MaxKeyedProcessFunction;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.MedianAggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class JoinWindowJointExecution extends QueryGroup<Tuple2<EntityRecordFull, EntityRecordFull>> {

    @Override
    public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        env.setParallelism(getParallelism()*2);
        var size = Time.seconds(3);
        var stride = Time.seconds(3);
        DataStream<EntityRecordFull> source =
                QueryUtils.newSourceStream(
                                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism())
                        .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        DataStream<EntityRecordFull> entityA = QueryUtils.entityA(source);
        DataStream<EntityRecordFull> entityB = QueryUtils.entityB(source);

        DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult = entityA.join(entityB).where(e -> e.PK).equalTo(e -> e.FK).window(SlidingEventTimeWindows.of(size, stride)).apply(new JoinFunction<EntityRecordFull, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
            @Override
            public Tuple2<EntityRecordFull, EntityRecordFull> join(EntityRecordFull first, EntityRecordFull second) throws Exception {
                return new Tuple2<>(first, second);
            }
        });

        var result1 = joinResult
                .keyBy(e -> e.f0.longAttribute1)
                .process(new MaxKeyedProcessFunction()).setParallelism(getParallelism());

        var result2 = joinResult
                .keyBy(e -> e.f1.longAttribute1)
                .window(SlidingEventTimeWindows.of(size, Time.seconds(1)))
                .aggregate(new MedianAggregateFunction()).setParallelism(getParallelism()).name("MedianWindowFunction");


        return new ArrayList<>() {
            {
                add(result1);
                add(result2);
            }
        };
    }

    @Override
    public String getName() {
        return "Join -- Joint Execution";
    }
}
