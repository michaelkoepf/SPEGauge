package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;

import java.util.LinkedList;
import java.util.List;

import net.michaelkoepf.spegauge.api.sut.BitSet;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.QuerySetAssignerImpl;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTestQuery extends QueryGroup<Tuple2<Long, BitSet>> {

    @Override
    public List<DataStream> register(StreamExecutionEnvironment env) {
        env.setMaxParallelism(4);
        Time WINDOW_SIZE = Time.seconds(3);

        /*
         * Query
         */
        DataStream<EntityRecordFull> source = QueryUtils.newSourceStream(
                        env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                .rebalance();

        DataStream<EntityRecordFull> entityA = QueryUtils.entityA(getGroupId(), source, getParallelism())
                .flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                .setParallelism(getParallelism()).name("QuerySetAssignerGID" + getGroupId());

        DataStream<Tuple2<Long, BitSet>> sharedResult =
                entityA
                        .keyBy(
                                (KeySelector<EntityRecordFull, Long>) record -> record.selectivityAttribute1)
                        .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                        .process(
                                new ProcessWindowFunction<EntityRecordFull, Tuple2<Long, BitSet>, Long, TimeWindow>() {
                                    private ValueState<Long> state;

                                    @Override
                                    public void open(Configuration conf) throws Exception {
                                        state =
                                                getRuntimeContext()
                                                        .getState(new ValueStateDescriptor<>("myState", Long.class));
                                    }

                                    @Override
                                    public void process(Long key, Context ctx, Iterable<EntityRecordFull> events, Collector<Tuple2<Long, BitSet>> out)
                                            throws Exception {

                                        // retrieve the current count
                                        Long current = state.value();
                                        // TODO this is weird
                                        BitSet querySet = null;
                                        if (current == null) {
                                            current = 0L;
                                        }
                                        // iterate over events
                                        for (EntityRecordFull event : events) {
                                            if (event.longAttribute1 > current) {
                                                current = event.longAttribute1;
                                                querySet = event.querySet;
                                            }
                                        }
                                        // write the state back
                                        state.update(current);

                                        if (querySet != null) {
                                            out.collect(new Tuple2<>(current, querySet));
                                        }
                                    }
                                })
                        .setParallelism(getParallelism())
                        .disableChaining()
                        .name("ProcessFunctionGID" + getGroupId());

        DataStream<Tuple2<Long, BitSet>> result = sharedResult
                .filter(record -> record.f1.get(0))
                .name("QueryFilterQID0GID" + getGroupId())
                .setParallelism(getParallelism())
                .keyBy(
                        new KeySelector<Tuple2<Long, BitSet>, Long>() {
                            @Override
                            public Long getKey(Tuple2<Long, BitSet> record) throws Exception {
                                return record.f0;
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .sum(0)
                .name("StatefulWindowSumQID0GID" + getGroupId())
                .setParallelism(2)
                .disableChaining();

        DataStream<Tuple2<Long, BitSet>> result2 = sharedResult
                .filter(record -> record.f1.get(1))
                .name("QueryFilterQID1GID" + getGroupId())
                .setParallelism(getParallelism())
                .keyBy(
                        new KeySelector<Tuple2<Long, BitSet>, Long>() {
                            @Override
                            public Long getKey(Tuple2<Long, BitSet> record) throws Exception {
                                return record.f0;
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .sum(0)
                .name("StatefulWindowSumQID1GID" + getGroupId())
                .setParallelism(2)
                .disableChaining();

        return new LinkedList<DataStream>() {
            {
                add(result);
            }
        };
    }

    @Override
    public String getName() {
        return "Window Test SQBench Query";
    }
}
