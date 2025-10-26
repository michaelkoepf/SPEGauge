package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.sut.DataRange;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.LinkedList;
import java.util.List;

public class ConfigurableStreamingETLQuery extends QueryGroup<Object> {

  public StreamingETLQuery queryConfig;
  private List<StreamingETLQuery> allConfigs; // all downstream operators for streaming etl queries

  private int QIDoffset;

  @Override
  public String getName() {
    return "CONFIGURABLE_STREAMING_ETL_QUERY_GID" + getGroupId();
  }

  public void setQueryConfig(StreamingETLQuery queryConfig, List<StreamingETLQuery> allConfigs, int QIDoffset) {
    this.queryConfig = queryConfig;
    this.allConfigs = allConfigs;
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
    for (StreamingETLQuery config : allConfigs) {
      if (config.filter != null) {
        filtersPresent = true;
        break;
      }
    }

    DataStream<EntityRecordFull> source =
            QueryUtils.newSourceStream(
                            env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                    .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

    DataStream<EntityRecordFull> mappedStream = null;

    // shareable operator
    switch (queryConfig.extractOperation.operationType) {
      case EXTRACT_JSON:
        DataStream<EntityRecordFull> querySetAssignerStream;
        if (!filtersPresent) {
          querySetAssignerStream = source
                  .flatMap(new QuerySetAssignerImpl(getGroupId(), getNumOfQueries()))
                  .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());
        }
        else {
          DataRange dataRanges[] = new DataRange[getNumOfQueries()];
          for (int i = 0; i < getNumOfQueries(); i++) {
            dataRanges[i] = new DataRange(
                    allConfigs.get(i).filter.startForEntityB, allConfigs.get(i).filter.endForEntityB);
          }
          querySetAssignerStream = source
                  .flatMap(new QuerySetAssignerDataRange(getGroupId(), getNumOfQueries(), dataRanges))
                  .setParallelism(getParallelism()).name("QuerySetAssignerBGID" + getGroupId());
        }
        mappedStream = querySetAssignerStream
                    .map(new JSONToPOJO()).setParallelism(getParallelism()).name("JSONToPOJOGID" + getGroupId())
                    .disableChaining();
        break;
      default:
        throw new IllegalStateException("Unknown extract operation for streaming ETL query: " + queryConfig.extractOperation);
    }

    List<DataStream> result = new LinkedList<>();
    for (int i = 0; i < allConfigs.size(); i++) {
      if (!(((Shareable) queryConfig).isShareable(allConfigs.get(i)))) {
        // not shareable because upstream operators are different -- skip
        continue;
      }

      final int qid = i + QIDoffset; // TODO: do we need an offset here?

      var downstreamOperator = allConfigs.get(i).downstreamOperator;
      final EntityRecordFull.Type filterField = (EntityRecordFull.Type) downstreamOperator.filterField();

      if (downstreamOperator instanceof DataEnrichtmentFunction) {
        MapFunction enrichmentFunction = null;
        String operatorName = null;
        switch (((DataEnrichtmentFunction) downstreamOperator).enrichmentType) {
          case SIMULATED_ENRICHMENT:
            enrichmentFunction = new DataEnrichmentFunction();
            operatorName = (downstreamOperator.isStateful() ? "Stateful" : "") + "EnrichmentFunction";
            break;
          default:
            throw new IllegalStateException("Unknown enrichment type for data analytics query: " + ((DataEnrichtmentFunction) downstreamOperator).enrichmentType);
        }

        var res = mappedStream
                // both records of the join result must be relevant for the query
                .filter(r -> r.querySet.get(qid))
                .name("QueryFilterQID" + qid + "GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .filter(entity -> entity.type == filterField).name("FilterQID" + qid + "GID" + getGroupId())  // TODO: how to deal with the operator name and the chaining here?
                .disableChaining()
                .map(enrichmentFunction)
                .setParallelism(getDownstreamParallelism())
                .name(operatorName + "QID" + qid + "GID" + getGroupId());

        result.add(res);
      } else if (downstreamOperator instanceof WindowedSumAndPOJOToX) {
        MapFunction mapToTargetFunction = null;
        String operatorName = null;
        SlidingEventTimeWindows window;

        switch (((WindowedSumAndPOJOToX) downstreamOperator).targetType) {
          case POJO_TO_AVRO:
            mapToTargetFunction = new POJOToAvro();
            operatorName = (downstreamOperator.isStateful() ? "Stateful" : "") + "SumAndPOJOToAvro";
            Time size = Time.milliseconds(((WindowAggregation) downstreamOperator).windowSizeMs);
            Time slide = Time.milliseconds(((WindowAggregation) downstreamOperator).windowSlideMs);
            window = SlidingEventTimeWindows.of(size, slide);
            break;
          default:
            throw new IllegalStateException("Unknown aggregation type for data analytics query: " + ((WindowedSumAndPOJOToX) downstreamOperator).targetType);
        }

        var res = mappedStream
                // both records of the join result must be relevant for the query
                .filter(r -> r.querySet.get(qid))
                .name("QueryFilterQID" + qid + "GID" + getGroupId())
                .setParallelism(getDownstreamParallelism())
                .filter(entity -> entity.type == filterField).name("FilterQID" + qid + "GID" + getGroupId())  // TODO: how to deal with the operator name and the chaining here?
                .disableChaining()
                .keyBy(e -> e.PK).window(window)
                .aggregate(new SumAggregationFunction())// TODO: how to deal with the operator name and the chaining here?
//                .disableChaining() // TODO: needed
                .map(mapToTargetFunction)
                .name(operatorName + "QID" + qid + "GID" + getGroupId())
                .setParallelism(getDownstreamParallelism());

        result.add(res);
      } else {
        throw new IllegalStateException("Unknown downstream operator for data analytics query: " + downstreamOperator.getClass().getName());
      }
    }

    return result;
  }
}
