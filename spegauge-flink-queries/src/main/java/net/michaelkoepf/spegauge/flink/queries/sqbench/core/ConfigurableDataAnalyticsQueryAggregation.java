package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import net.michaelkoepf.spegauge.api.sut.BitSet;
import net.michaelkoepf.spegauge.api.sut.DataRange;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * WARNING: This works only if queries have the same downstream operators!!!
 * It does not check the query sets after the join
 * To do that:
 * 1. Uncomment the filters between the join and the downstream operators
 * 2. Create multiple SideOutputs for all the queries (maybe all but one which will use the normal output?)
 * 3. Replace the CoGroupFunction with a RichCoGroupFunction. In the RichCoGroupFunction, check the query sets and
 * propagate the records to the correct SideOutput
 */
public class ConfigurableDataAnalyticsQueryAggregation extends QueryGroup<Object> {

    public DataAnalyticsQuery queryConfig;
    private List<DataAnalyticsQuery> allConfigs; // all downstream operators for data analytics queries

    private int QIDoffset;

    @Override
    public String getName() {
        return "CONFIGURABLE_DATA_ANALYTICS_QUERY_GID" + getGroupId();
    }

    public void setQueryConfig(DataAnalyticsQuery thisQuery, List<DataAnalyticsQuery> allQueries, int QIDoffset) {
        this.queryConfig = thisQuery;
        this.allConfigs = allQueries;
        this.QIDoffset = QIDoffset;
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        if (queryConfig == null) {
            throw new IllegalStateException("QueryConfig is not set");
        }

        if (allConfigs == null || allConfigs.isEmpty()) {
            throw new IllegalStateException("DownstreamOperators are not set");
        }

        boolean filtersPresent = false;
        for (DataAnalyticsQuery config : allConfigs) {
            if (config.filter != null) {
                filtersPresent = true;
                break;
            }
        }

        List<OutputTag<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>> outputTags = new ArrayList<>(getNumOfQueries()-1);
        for (int i = 1; i < getNumOfQueries(); i++) {
            outputTags.add(new OutputTag<>("output" + i) {});
        }

        DataStream<EntityRecordLimitedAttrs> source =
                QueryUtils.newSourceStreamJoinNoExtraAttrs(
                                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                        .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        DataStream<EntityRecordLimitedAttrs> entityA = QueryUtils.entityAJoin(getGroupId(), source, getParallelism());
        DataStream<EntityRecordLimitedAttrs> entityAQuerySet;
        if (!filtersPresent) {
            entityAQuerySet = entityA.flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                    .setParallelism(getParallelism()).name("QuerySetAssignerAGID" + getGroupId());
        }
        else {
            DataRange dataRangesEntityA[] = new DataRange[getNumOfQueries()];
            for (int i = 0; i < getNumOfQueries(); i++) {
                dataRangesEntityA[i] = new DataRange(
                        allConfigs.get(i).filter.startForEntityA, allConfigs.get(i).filter.endForEntityA);
            }
            entityAQuerySet = entityA.flatMap(new QuerySetAssignerDataRangeMonitoring(getGroupId(), getNumOfQueries(), dataRangesEntityA))
                    .setParallelism(getParallelism()).name("QuerySetAssignerAGID" + getGroupId());
        }
        // f0 is the attribute used for the mean, f1 is the attribute used for filter & groupBy, f2 is the querySet
        DataStream<Tuple3<Integer, Integer, BitSet>> entityANoUnnecessaryAttrs = entityAQuerySet.map(
                new MapFunction<EntityRecordLimitedAttrs, Tuple3<Integer, Integer, BitSet>>() {
                    @Override
                    public Tuple3<Integer, Integer, BitSet> map(EntityRecordLimitedAttrs rec) throws Exception {
                        return new Tuple3<Integer, Integer, BitSet>(rec.intAttribute, rec.filterAttribute, rec.querySet);
                    }
                }).setParallelism(getParallelism()).name("AttrFilterAGID" + getGroupId());



        DataStream<Tuple3<Integer, Integer, BitSet>> aggrResult;

        AggregateFunction aggregationFunction = new MedianAggregateFunctionNoExtraAttrs();

        aggrResult = entityANoUnnecessaryAttrs
                .keyBy(e -> e.f1)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(queryConfig.aggregationOperator.windowSizeMs),
                        Time.milliseconds(queryConfig.aggregationOperator.windowSlideMs)))
                .aggregate(aggregationFunction)
                .setParallelism(getParallelism())
                .name("StatefulAggregationGID" + getGroupId())
                .disableChaining();

        // attach downstream operators
        List<DataStream> result = new LinkedList<>();
        result.add(aggrResult);


        return result;
    }
}
