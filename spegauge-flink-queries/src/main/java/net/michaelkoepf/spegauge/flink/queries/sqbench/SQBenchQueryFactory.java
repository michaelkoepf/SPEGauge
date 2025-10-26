package net.michaelkoepf.spegauge.flink.queries.sqbench;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class SQBenchQueryFactory {

  private static final Map<String, Supplier<QueryGroup>> FACTORY =
      Collections.unmodifiableMap(
          // LinkedHashMap to preserve order when getting keys for IMPLEMENTED_QUERIES (because it
          // used when printing the help message; otherwise, this will be confusing for the user)
          new LinkedHashMap<String, Supplier<QueryGroup>>() {
            {
              put("TWO_WAY_WINDOW_JOIN_Q1", JoinWindowQ1::new);
              put("TWO_WAY_WINDOW_JOIN_Q2", JoinWindowQ2::new);
              put("TWO_WAY_WINDOW_JOIN_JOINT_EXECUTION", JoinWindowJointExecution::new);
              put("THREE_WAY_WINDOW_JOIN_Q1", ThreeWayJoinWindowQ1::new);
              put("THREE_WAY_WINDOW_JOIN_Q2", ThreeWayJoinWindowQ2::new);
              put("THREE_WAY_WINDOW_JOIN_JOINT_EXECUTION", ThreeWayJoinWindowJointExecution::new);
              put("STREAMING_ETL_Q1", StreamingETLQ1::new);
              put("STREAMING_ETL_Q2", StreamingETLQ2::new);
              put("STREAMING_ETL_JOINT_EXECUTION", StreamingETLJointExecution::new);
              put("SCALABILITY", Scalability::new);
              put("MAP_EXTRACT_JSON", MapExtractJSON::new);
              put("TEST_QUERY", TestQuery::new);
              put("WINDOW_TEST_QUERY", WindowTestQuery::new);
              put("CONFIGURABLE_DATA_ANALYTICS_QUERY", ConfigurableDataAnalyticsQuery::new);
              put("CONFIGURABLE_STREAMING_ETL_QUERY", ConfigurableStreamingETLQuery::new);
            }
          });

  public static final Set<String> IMPLEMENTED_QUERIES =
      Collections.unmodifiableSet(FACTORY.keySet());

  private SQBenchQueryFactory() {}

  public static QueryGroup<?> getQuery(
      String queryName, StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries,
      int parallelism, int downstreamParallelism) {
    Supplier<QueryGroup> supplier = FACTORY.get(queryName);

    if (supplier == null) {
      throw new IllegalStateException("Unknown query " + queryName);
    } else {
      QueryGroup<?> query = supplier.get();
      query.initialize(env, hostname, port, groupId, numOfQueries, parallelism, downstreamParallelism);
      return query;
    }
  }

  public static QueryGroup<?> getQuery(
          String queryName, StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries) {
    return getQuery(queryName, env, hostname, port, groupId, numOfQueries, env.getParallelism(), env.getParallelism());
  }
}
