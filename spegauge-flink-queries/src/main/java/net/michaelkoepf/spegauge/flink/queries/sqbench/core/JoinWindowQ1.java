package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.MaxKeyedProcessFunction;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.QuerySetAssignerImpl;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class JoinWindowQ1 extends QueryGroup<Tuple2<EntityRecordFull, EntityRecordFull>> {

    @Override
    public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        var size = Time.seconds(3);
        var stride = Time.seconds(3);
        DataStream<EntityRecordFull> source =
                QueryUtils.newSourceStream(
                        env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                        .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        DataStream<EntityRecordFull> entityA = QueryUtils.entityA(getGroupId(), source, getParallelism())
                .flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                .setParallelism(getParallelism()).name("QuerySetAssignerGID" + getGroupId());
        DataStream<EntityRecordFull> entityB = QueryUtils.entityB(getGroupId(), source, getParallelism())
                .flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());

        DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult = entityA
                .join(entityB)
                .where(e -> e.PK)
                .equalTo(e -> e.FK)
                .window(SlidingEventTimeWindows.of(size, stride))
                .with(new FlatJoinFunction<EntityRecordFull, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
                    @Override
                    public void join(EntityRecordFull first, EntityRecordFull second, Collector<Tuple2<EntityRecordFull, EntityRecordFull>> out) throws Exception {
                        out.collect(new Tuple2<>(first, second));
                    }
                })
                .setParallelism(getParallelism())
                .name("StatefulJoinGID" + getGroupId())
                .disableChaining();

        // TODO for now I am adding 2 identical downstream operators. We must refactor such that we can add here the donwstream operators of the currently running queries
        // add window after key by
        var resultQuery0 = joinResult
                // both records of the join result must be relevant for the query
                .filter(joinRecord -> joinRecord.f0.querySet.get(0) && joinRecord.f1.querySet.get(0))
                .name("QueryFilterQID0GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .keyBy(e -> e.f0.longAttribute1)
                .process(new MaxKeyedProcessFunction())
                .name("StatefulMaxQID0GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .disableChaining();

        var resultQuery1 = joinResult
                // both records of the join result must be relevant for the query
                .filter(joinRecord -> joinRecord.f0.querySet.get(1) && joinRecord.f1.querySet.get(1))
                .name("QueryFilterQID1GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .keyBy(e -> e.f0.longAttribute1)
                .process(new MaxKeyedProcessFunction())
                .name("StatefulMaxQID1GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .disableChaining();

        return new ArrayList<>() {
            {
                add(resultQuery0);
                add(resultQuery1);
            }
        };
    }

    @Override
    public String getName() {
        return "Join -- Q1";
    }
}
