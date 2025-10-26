package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.metrics.MeasuredEntity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapExtractJSON extends QueryGroup<MeasuredEntity> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    DataStream<EntityRecordFull> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism())
            .rebalance();

    DataStream<MeasuredEntity> result = source.map(MeasuredEntity::new).map(new MapFunction<MeasuredEntity, MeasuredEntity>()  {
      private static final long serialVersionUID = 1L;
      private final ObjectMapper mapper = new ObjectMapper();

      @Override
      public MeasuredEntity map(MeasuredEntity value) throws Exception {
        long begin = System.nanoTime();
        EntityRecordFull entity = value.getEntity();
        TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<>() {};
        Map<String, Object> jsonMap = mapper.readValue(entity.jsonString, typeRef);
        entity.jsonString = jsonMap.get("firstName") + " " + jsonMap.get("lastName");
        value.setDuration(System.nanoTime() - begin);
        return value;
      }
    });

    return new LinkedList<DataStream>() {
      {
        add(result);
      }
    };
  }

  @Override
  public String getName() {
    return "Map Extract JSON";
  }
}
