package net.michaelkoepf.spegauge.flink.queries.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public abstract class QueryGroup<T> {

  // see
  // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/#set-slot-sharing-group
  private static final String DEFAULT_SLOT_SHARING_GROUP = "default";

  private StreamExecutionEnvironment env;

  private boolean initialized = false;

  @Getter @Setter private String slotSharingGroup = DEFAULT_SLOT_SHARING_GROUP;

  @Getter private String hostname;

  @Getter private int port;

  @Getter private int sourceParallelism;

  @Getter private int parallelism;

  @Getter private int downstreamParallelism;

  @Getter private int watermarkInterval;

  @Getter private Time windowSize;

  @Getter private Time windowSlide;

  @Getter private int groupId;

  // the number of concurrent queries
  @Getter private int numOfQueries;

  public QueryGroup() {}

  public void initialize(StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries,
                         int parallelism, int downstreamParallelism) {
    this.env = env;
    this.hostname = hostname;
    this.port = port;
    this.groupId = groupId;
    this.numOfQueries = numOfQueries;

    ParameterTool jobParameters = (ParameterTool) this.env.getConfig().getGlobalJobParameters();
    sourceParallelism = jobParameters.getInt("source.parallelism");
    this.parallelism = parallelism;
    this.downstreamParallelism = downstreamParallelism;

    watermarkInterval = jobParameters.getInt("watermark.interval");

    initialized = true;
  }

  public void initialize(StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries) {
    initialize(env, hostname, port, groupId, numOfQueries, env.getParallelism(), env.getParallelism());
  }

  public abstract String getName();

  public List<DataStream> register() throws Exception {
    if (!initialized) {
      throw new IllegalStateException("Query was not initialized. Did you used the QueryFactory");
    }
    return register(env);
  }

  protected abstract List<DataStream> register(StreamExecutionEnvironment env) throws Exception;
}
