package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.sut.DataRange;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;

public class ConfigurableDataAnalyticsQuery extends QueryGroup<Object> {

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

    DataStream<EntityRecordFull> source =
            QueryUtils.newSourceStream(
                            env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                    .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

    // Join between entity A and entity B
    DataStream<EntityRecordFull> entityA = QueryUtils.entityA(getGroupId(), source, getParallelism());
    DataStream<EntityRecordFull> entityAQuerySet;
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
      entityAQuerySet = entityA.flatMap(new QuerySetAssignerDataRange(getGroupId(), getNumOfQueries(), dataRangesEntityA))
              .setParallelism(getParallelism()).name("QuerySetAssignerAGID" + getGroupId());
    }

    DataStream<EntityRecordFull> entityB = QueryUtils.entityB(getGroupId(), source, getParallelism());
    DataStream<EntityRecordFull> entityBQuerySet;
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
      entityBQuerySet = entityB.flatMap(new QuerySetAssignerDataRange(getGroupId(), getNumOfQueries(), dataRangesEntityB))
              .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());
    }

    DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult;
    /* For the two-way join we must disable chaining after the join because directly after the join we have the downstream operators.
       We want to be able to freely change the parallelism of the join without affecting the downstream operators.

       For the three-way join there is no need to disable chaining after the first join. Even further, it is better to chain
       both for performance reasons and to be able to follow the naming convention required by the controller.
     */

    if (queryConfig.joinOperators.joinType == WindowJoin.JoinType.THREE_WAY_EQUI_JOIN) {
      joinResult = entityAQuerySet
              .join(entityBQuerySet)
              .where(e -> e.PK)
              .equalTo(e -> e.FK)
              .window(SlidingEventTimeWindows.of(Time.milliseconds(queryConfig.joinOperators.windowSizeMs), Time.milliseconds(queryConfig.joinOperators.windowSlideMs)))
              .with(new FlatJoinFunction<EntityRecordFull, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
                @Override
                public void join(EntityRecordFull first, EntityRecordFull second, Collector<Tuple2<EntityRecordFull, EntityRecordFull>> out) throws Exception {
                  out.collect(new Tuple2<>(first, second));
                }
              })
              .setParallelism(getParallelism())
              .name("StatefulJoinGID" + getGroupId());
    }
    else {
      joinResult = entityAQuerySet
              .join(entityBQuerySet)
              .where(e -> e.PK)
              .equalTo(e -> e.FK)
              .window(SlidingEventTimeWindows.of(Time.milliseconds(queryConfig.joinOperators.windowSizeMs), Time.milliseconds(queryConfig.joinOperators.windowSlideMs)))
              .with(new FlatJoinFunction<EntityRecordFull, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
                @Override
                public void join(EntityRecordFull first, EntityRecordFull second, Collector<Tuple2<EntityRecordFull, EntityRecordFull>> out) throws Exception {
                  out.collect(new Tuple2<>(first, second));
                }
              })
              .setParallelism(getParallelism())
              .name("StatefulJoinGID" + getGroupId())
              .disableChaining();
    }

    // add second join (if three-way join)
    if (queryConfig.joinOperators.joinType == WindowJoin.JoinType.THREE_WAY_EQUI_JOIN) {
      DataStream<EntityRecordFull> entityC = QueryUtils.entityC(getGroupId(), source, getParallelism());
      DataStream<EntityRecordFull> entityCQuerySet;
      if (!filtersPresent) {
        entityCQuerySet = entityC.flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                .setParallelism(getParallelism()).name("QuerySetAssignerCGID" + getGroupId());
      }
      else {
        DataRange dataRangesEntityC[] = new DataRange[getNumOfQueries()];
        for (int i = 0; i < getNumOfQueries(); i++) {
          dataRangesEntityC[i] = new DataRange(
                  allConfigs.get(i).filter.startForEntityC, allConfigs.get(i).filter.endForEntityC);
        }
        entityCQuerySet = entityC.flatMap(new QuerySetAssignerDataRange(getGroupId(), getNumOfQueries(), dataRangesEntityC))
                .setParallelism(getParallelism()).name("QuerySetAssignerCGID" + getGroupId());
      }

      joinResult = joinResult
              .join(entityCQuerySet)
              .where(e -> e.f1.PK)
              .equalTo(e -> e.FK)
              .window(SlidingEventTimeWindows.of(Time.milliseconds(queryConfig.joinOperators.windowSizeMs), Time.milliseconds(queryConfig.joinOperators.windowSlideMs)))
              .with(new FlatJoinFunction<Tuple2<EntityRecordFull, EntityRecordFull>, EntityRecordFull, Tuple2<EntityRecordFull, EntityRecordFull>>() {
                @Override
                public void join(Tuple2<EntityRecordFull, EntityRecordFull> first, EntityRecordFull second, Collector<Tuple2<EntityRecordFull, EntityRecordFull>> out) throws Exception {
                  out.collect(new Tuple2<>(first.f0, second));
                }
              })
              .setParallelism(getParallelism())
              .name("StatefulJoin2GID" + getGroupId())
              .disableChaining();
    }

    // attach downstream operators
    List<DataStream> result = new LinkedList<>();

    for (int i = 0; i < allConfigs.size(); i++) {
      if (!(((Shareable) queryConfig).isShareable(allConfigs.get(i)))) {
        // not shareable because upstream operators are different -- skip
        continue;
      }

      final int qid = i + QIDoffset; // TODO: do we need an offset here?

      var downstreamOperator = allConfigs.get(i).downstreamOperator;
      if (downstreamOperator == null) {
        result.add(joinResult);
        continue;
      }

      final String groupByField = (String) downstreamOperator.groupByField();

      if (downstreamOperator instanceof RollingReduce) {
        KeyedProcessFunction aggregationFunction = null;
        String aggregationFunctionNamePrefix = null;
        switch (((RollingReduce) downstreamOperator).aggregationType) {
          case MAX:
            aggregationFunction = new MaxKeyedProcessFunction();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "Max";
            break;
          default:
            throw new IllegalStateException("Unknown aggregation type for data analytics query: " + ((RollingReduce) downstreamOperator).aggregationType);
        }

        var res = joinResult
            // both records of the join result must be relevant for the query
            .filter(joinRecord -> joinRecord.f0.querySet.get(qid) && joinRecord.f1.querySet.get(qid))
            .name("QueryFilterQID" + qid + "GID" + getGroupId())
            .setParallelism(getDownstreamParallelism())
                .keyBy(e -> {
                  if (groupByField.equals("f0")) {
                    return e.f0.longAttribute1;
                  } else if (groupByField.equals("f1")) {
                    return e.f1.longAttribute1;
                  } else {
                    throw new IllegalStateException("Unknown group by field for data analytics query: " + groupByField);
                  }
                })
            .process(aggregationFunction)
            .name(aggregationFunctionNamePrefix + "QID" + qid + "GID" + getGroupId())
            .setParallelism(getDownstreamParallelism())
            .disableChaining();
        result.add(res);
      } else if (downstreamOperator instanceof WindowAggregation) {
        AggregateFunction aggregationFunction = null;
        String aggregationFunctionNamePrefix = null;
        switch (((WindowAggregation) downstreamOperator).aggregationType) {
          case MEAN:
            aggregationFunction = new MeanAggregationFunction();
            aggregationFunctionNamePrefix =  (downstreamOperator.isStateful() ? "Stateful" : "") + "MeanWindow";
            break;
          case MEDIAN:
            aggregationFunction = new MedianAggregateFunction();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "MedianWindow";
            break;
          case COUNT:
            aggregationFunction = new CountAggregateFunction();
            aggregationFunctionNamePrefix = (downstreamOperator.isStateful() ? "Stateful" : "") + "CountWindow";
            break;
          default:
            throw new IllegalStateException("Unknown aggregation type for data analytics query: " + ((WindowAggregation) downstreamOperator).aggregationType);
        }

        var res = joinResult
            .filter(joinRecord -> joinRecord.f0.querySet.get(qid) && joinRecord.f1.querySet.get(qid))
            .name("QueryFilterQID" + qid + "GID" + getGroupId())
            .setParallelism(getDownstreamParallelism())
            .keyBy(e -> {
              if (groupByField.equals("f0")) {
                return e.f0.longAttribute1;
              } else if (groupByField.equals("f1")) {
                return e.f1.longAttribute1;
              } else {
                throw new IllegalStateException("Unknown group by field for data analytics query: " + groupByField);
              }
            })
            .window(SlidingEventTimeWindows.of(Time.milliseconds(((WindowAggregation) downstreamOperator).windowSizeMs), Time.milliseconds(((WindowAggregation) downstreamOperator).windowSlideMs)))
            .aggregate(aggregationFunction)
            .setParallelism(getDownstreamParallelism())
            .name(aggregationFunctionNamePrefix + "QID" + qid + "GID" + getGroupId())
            .disableChaining();

        result.add(res);
      } else {
        throw new IllegalStateException("Unknown downstream operator for data analytics query: " + downstreamOperator.getClass().getName());
      }
    }

    return result;
  }
}
