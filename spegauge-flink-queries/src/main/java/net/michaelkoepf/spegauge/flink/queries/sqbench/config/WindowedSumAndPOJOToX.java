package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.NotNull;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class WindowedSumAndPOJOToX extends WindowAggregation implements DownstreamOperator {
  public enum TargetType {
    POJO_TO_AVRO
  }


  @JsonProperty
  @NotNull
  public TargetType targetType;

  @JsonProperty
  @NotNull
  public EntityRecordFull.Type filterField;

  public boolean isStateful() {
    return true;
  }

  public Object filterField() {
    return this.filterField;
  }
}
