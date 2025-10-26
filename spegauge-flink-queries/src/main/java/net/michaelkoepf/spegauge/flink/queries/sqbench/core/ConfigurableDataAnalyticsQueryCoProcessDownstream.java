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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ConfigurableDataAnalyticsQueryCoProcessDownstream extends QueryGroup<Object> {

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

    // Join between entity A and entity B
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
    DataStream<Tuple3<Integer, Integer, BitSet>> entityANoUnnecessaryAttrs = entityAQuerySet.map(
            new MapFunction<EntityRecordLimitedAttrs, Tuple3<Integer, Integer, BitSet>>() {
              @Override
              public Tuple3<Integer, Integer, BitSet> map(EntityRecordLimitedAttrs rec) throws Exception {
                return new Tuple3<Integer, Integer, BitSet>(rec.intAttribute, rec.filterAttribute, rec.querySet);
              }
            }).setParallelism(getParallelism()).name("AttrFilterAGID" + getGroupId());

    DataStream<EntityRecordLimitedAttrs> entityB = QueryUtils.entityBJoin(getGroupId(), source, getParallelism());
    DataStream<EntityRecordLimitedAttrs> entityBQuerySet;
    if (!filtersPresent) {
      entityBQuerySet = entityB.flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
              .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());
    }
    else {
      DataRange dataRangesEntityB[] = new DataRange[getNumOfQueries()];
      for (int i = 0; i < getNumOfQueries(); i++) {
        dataRangesEntityB[i] = new DataRange(
                allConfigs.get(i).filter.startForEntityB, allConfigs.get(i).filter.endForEntityB);
      }
      entityBQuerySet = entityB.flatMap(new QuerySetAssignerDataRangeMonitoring(getGroupId(), getNumOfQueries(), dataRangesEntityB))
              .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());
    }
    DataStream<Tuple3<Integer, Integer, BitSet>> entityBNoUnnecessaryAttrs = entityBQuerySet.map(
                    new MapFunction<EntityRecordLimitedAttrs, Tuple3<Integer, Integer, BitSet>>() {
                      @Override
                      public Tuple3<Integer, Integer, BitSet> map(EntityRecordLimitedAttrs rec) throws Exception {
                        return new Tuple3<Integer, Integer, BitSet>(rec.intAttribute, rec.filterAttribute, rec.querySet);
                      }
                    })
            .setParallelism(getParallelism()).name("AttrFilterBGID" + getGroupId());

    DataStream<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> joinResult;
    /* For the two-way join we must disable chaining after the join because directly after the join we have the downstream operators.
       We want to be able to freely change the parallelism of the join without affecting the downstream operators.

       For the three-way join there is no need to disable chaining after the first join. Even further, it is better to chain
       both for performance reasons and to be able to follow the naming convention required by the controller.
     */

    if (queryConfig.joinOperators.joinType == WindowJoin.JoinType.THREE_WAY_EQUI_JOIN) {
      throw new IllegalStateException("Three-way join not implemented yet");
    }
    else {
        joinResult = entityANoUnnecessaryAttrs
                .keyBy(e -> e.f0)
                .connect(entityBNoUnnecessaryAttrs.keyBy(e -> e.f0))
                .process(new CoProcessJoin(queryConfig.joinOperators.windowSizeMs,
                        queryConfig.joinOperators.windowSlideMs, getNumOfQueries(), outputTags))
                .setParallelism(getParallelism())
                .name("StatefulJoinGID" + getGroupId())
                .disableChaining();
    }

    // add second join (if three-way join)
    if (queryConfig.joinOperators.joinType == WindowJoin.JoinType.THREE_WAY_EQUI_JOIN) {
      throw new IllegalStateException("Three-way join not implemented yet");
    }

    // attach downstream operators
    List<DataStream> result = new LinkedList<>();

    //for (int i = 0; i < allConfigs.size(); i++) {
    int i = getGroupId();
      /*if (!(((Shareable) queryConfig).isShareable(allConfigs.get(i)))) {
        // not shareable because upstream operators are different -- skip
        continue;
      }*/

      final int qid = i + QIDoffset; // TODO: do we need an offset here?

      var downstreamOperator = allConfigs.get(i).downstreamOperator;
      if (downstreamOperator == null) {
        result.add(joinResult);
        //continue;
        return result;
      }

      if (downstreamOperator instanceof RollingReduce) {
        throw new RuntimeException("RollingReduce not supported");
      } else if (downstreamOperator instanceof WindowAggregation) {
        AggregateFunction aggregationFunction = null;
        String aggregationFunctionNamePrefix = null;
        switch (((WindowAggregation) downstreamOperator).aggregationType) {
          case MEAN:
            aggregationFunction = new MeanAggregationFunctionJoinAggr();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "MeanWindow";
            break;
          case MEDIAN:
            aggregationFunction = new MedianAggregateFunctionJoinAggr();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "MedianWindow";
            break;
          /*case COUNT:
            aggregationFunction = new CountAggregateFunction();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "CountWindow";
            break;*/
          default:
            throw new IllegalStateException("Unknown aggregation type for data analytics query: " +
                    ((WindowAggregation) downstreamOperator).aggregationType);

        }

        DataStream<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> inputToDownstream;
        if (qid == 0) {
          inputToDownstream = joinResult;
        } else {
          inputToDownstream = ((SingleOutputStreamOperator<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>)joinResult)
                  .getSideOutput(outputTags.get(qid - 1));
        }
        var res = inputToDownstream
                //.filter(joinRecord -> joinRecord.f0.f1.get(qid) && joinRecord.f1.f1.get(qid))
                //.name("QueryFilterQID" + qid + "GID" + getGroupId())
                //.setParallelism(getDownstreamParallelism())
                .keyBy(e -> e.f0.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(queryConfig.joinOperators.windowSlideMs)))
                .aggregate(aggregationFunction)
                .setParallelism(32)//(getDownstreamParallelism())
                .name(aggregationFunctionNamePrefix + "QID" + qid + "GID" + getGroupId())
                .disableChaining();
        result.add(res);

        } else {
        throw new IllegalStateException("Unknown downstream operator for data analytics query: " +
                downstreamOperator.getClass().getName());
      }
    //}
    return result;
  }
}
