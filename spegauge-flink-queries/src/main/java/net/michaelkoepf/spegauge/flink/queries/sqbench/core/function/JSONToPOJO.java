package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.MapFunction;

public class JSONToPOJO implements MapFunction<EntityRecordFull, EntityRecordFull> {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public EntityRecordFull map(EntityRecordFull value) throws Exception {
    if (value.type == EntityRecordFull.Type.JSON) {

      EntityRecordFull result =  mapper.readValue(value.jsonString, EntityRecordFull.class);

      assert result != null;
      assert result.type != EntityRecordFull.Type.JSON;

      result.querySet = value.querySet;
      return result;
    } else {
      throw new RuntimeException("Invalid entity type " + value.type);
    }
  }
}