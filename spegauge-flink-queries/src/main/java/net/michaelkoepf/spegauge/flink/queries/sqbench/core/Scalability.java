package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.LinkedList;
import java.util.List;

public class Scalability extends QueryGroup<EntityRecordFull> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    DataStream<EntityRecordFull> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism());

    var size = Time.seconds(3);
    var stride = Time.seconds(3);

    var result1 = source.map(e -> e).disableChaining().setParallelism(getParallelism());

//    DataStream<EntityRecord> mapExtractJSON = source.map(new MapFunction<EntityRecord, EntityRecord>() {
//
//      private final ObjectMapper mapper = new ObjectMapper();
//
//      @Override
//      public EntityRecord map(EntityRecord value) throws Exception {
//        if (value.type == EntityRecord.Type.JSON) {
//
//          EntityRecord parsedRecord = mapper.readValue(value.jsonString, EntityRecord.class);
//
//          return parsedRecord;
//        } else {
//          throw new RuntimeException("Invalid entity type " + value.type);
//        }
//      }
//    }).setParallelism(2*getParallelism()).disableChaining();

//    var result1 = mapExtractJSON.filter(entity -> entity.type == EntityRecord.Type.A).map(new MapFunction<EntityRecord, EntityRecord>() {
//
//      @Override
//      public EntityRecord map(EntityRecord value) throws Exception {
//          long start = System.nanoTime();
//          while (System.nanoTime() - start < 1e8) {
//            // busy wait
//          }
//        return value;
//      }
//    }).setParallelism(getParallelism());
//
//    var result2 = mapExtractJSON.filter(entity -> entity.type == EntityRecord.Type.B).keyBy(e -> e.PK).window(SlidingEventTimeWindows.of(size, stride)).sum("longAttribute1").setParallelism(getParallelism());

    return new LinkedList<DataStream>() {
      {
        add(result1);
//        add(result2);
      }
    };
  }

  @Override
  public String getName() {
    return "Map Multiplication";
  }
}
