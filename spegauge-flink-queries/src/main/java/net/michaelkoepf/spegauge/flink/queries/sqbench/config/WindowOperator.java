package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.NotNull;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public abstract class WindowOperator extends Operator {

  @JsonProperty
  @NotNull
  public int windowSizeMs;

  @JsonProperty
  @NotNull
  public int windowSlideMs;

}
