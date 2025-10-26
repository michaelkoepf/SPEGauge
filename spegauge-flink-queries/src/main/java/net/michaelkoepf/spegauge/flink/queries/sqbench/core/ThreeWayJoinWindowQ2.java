package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.MedianAggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class ThreeWayJoinWindowQ2 extends QueryGroup<Tuple2<EntityRecordFull, EntityRecordFull>> {

    @Override
    public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        var size = Time.seconds(3);
        var stride = Time.seconds(3);
        DataStream<EntityRecordFull> source =
                QueryUtils.newSourceStream(
                        env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism()).rebalance();

        DataStream<EntityRecordFull> entityA = QueryUtils.entityA(source);
        DataStream<EntityRecordFull> entityB = QueryUtils.entityB(source);
        DataStream<EntityRecordFull> entityC = QueryUtils.entityC(source);

        DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult = entityA.join(entityB).where(e -> e.PK).equalTo(e -> e.FK).window(SlidingEventTimeWindows.of(size, stride)).apply(new JoinFunction<EntityRecordFull, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
            @Override
            public Tuple2<EntityRecordFull, EntityRecordFull> join(EntityRecordFull first, EntityRecordFull second) throws Exception {
                return new Tuple2<>(first, second);
            }
        }).join(entityC).where(e -> e.f1.PK).equalTo(e -> e.FK).window(SlidingEventTimeWindows.of(size, stride)).apply(new JoinFunction<Tuple2<EntityRecordFull, EntityRecordFull>, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
            @Override
            public Tuple2<EntityRecordFull, EntityRecordFull> join(Tuple2<EntityRecordFull, EntityRecordFull> first, EntityRecordFull second) throws Exception {
                return new Tuple2<>(first.f0, second);
            }
        });

        // add window after key by
        var result1 = joinResult
                .keyBy(e -> e.f1.longAttribute1)
                .window(SlidingEventTimeWindows.of(size, Time.seconds(1)))
                .aggregate(new MedianAggregateFunction()).setParallelism(getParallelism()).name("MedianWindowFunction");

        return new ArrayList<>() {
            {
                add(result1);
            }
        };
    }

    @Override
    public String getName() {
        return "3-Way Join -- Q2";
    }
}
