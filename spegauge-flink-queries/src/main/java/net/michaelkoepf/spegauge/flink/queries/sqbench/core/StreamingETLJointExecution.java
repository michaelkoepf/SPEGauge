package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.POJOToAvro;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.DataEnrichmentFunction;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.JSONToPOJO;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.SumAggregationFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.LinkedList;
import java.util.List;

public class StreamingETLJointExecution extends QueryGroup<Object> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    DataStream<EntityRecordFull> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism()).rebalance();

    var size = Time.seconds(3);
    var stride = Time.seconds(3);

    DataStream<EntityRecordFull> mapExtractJSON = source.map(new JSONToPOJO()).setParallelism(2*getParallelism()).name("JSON to POJO");

    var result1 = mapExtractJSON.filter(entity -> entity.type == EntityRecordFull.Type.A).name("Filter Entity A").map(new DataEnrichmentFunction()).setParallelism(getParallelism()).name("Data Enrichment");
    var result2 = mapExtractJSON.filter(entity -> entity.type == EntityRecordFull.Type.B).name("Filter Entity B")
            .keyBy(e -> e.PK).window(SlidingEventTimeWindows.of(size, stride))
            .aggregate(new SumAggregationFunction()).name("Window Sum by Key").map(new POJOToAvro()).name("POJO to Avro").setParallelism(getParallelism());

    return new LinkedList<DataStream>() {
      {
        add(result1);
        add(result2);
      }
    };
  }

  @Override
  public String getName() {
    return "Streaming ETL -- Joint Execution";
  }
}
