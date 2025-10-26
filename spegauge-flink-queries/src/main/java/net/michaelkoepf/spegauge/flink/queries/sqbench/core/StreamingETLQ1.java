package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.DataEnrichmentFunction;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.JSONToPOJO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

public class StreamingETLQ1 extends QueryGroup<Object> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    DataStream<EntityRecordFull> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism()).rebalance();

    DataStream<EntityRecordFull> mapExtractJSON = source.map(new JSONToPOJO()).name("JSON to POJO");

    var result1 = mapExtractJSON.filter(entity -> entity.type == EntityRecordFull.Type.A).name("Filter Entity A").map(new DataEnrichmentFunction()).name("Data Enrichment");

    return new LinkedList<DataStream>() {
      {
        add(result1);
      }
    };
  }

  @Override
  public String getName() {
    return "Streaming ETL -- Q1";
  }
}
