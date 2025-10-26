package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.SQBenchQueryFactory;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.Operator;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.StreamingETLQuery;
import net.michaelkoepf.spegauge.flink.sdk.source.AbstractParallelTCPSource;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceCSV;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceNoExtraAttrs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class QueryUtils {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QueryUtils.class);
  private static final AtomicInteger sourceNumber = new AtomicInteger();

  private QueryUtils() {}

  /**
   * Returns a new source that is in its own slot sharing group.
   *
   * @param env
   * @param hostname
   * @param port
   * @param watermarkInterval
   * @param parallelism
   * @return
   */
  public static DataStream<EntityRecordFull> newSourceStream(
      StreamExecutionEnvironment env,
      String hostname,
      int port,
      int watermarkInterval,
      int parallelism,
      int groupId) {
    // TODO: watermarkInterval is not used; can be removed
    AbstractParallelTCPSource<EntityRecordFull> source =
        new SQBenchParallelTCPSourceCSV(hostname, port, watermarkInterval);
    return env.addSource(source)
        .setParallelism(parallelism)
        .setMaxParallelism(parallelism)
            // TODO: not used at the moment (and also doesn't make a performance difference as far as I tested it)
//        .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("SQBenchParallelTCPSourceGID" + groupId);
  }

  public static DataStream<EntityRecordLimitedAttrs> newSourceStreamJoinNoExtraAttrs(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int watermarkInterval,
          int parallelism,
          int groupId) {
    // TODO: watermarkInterval is not used; can be removed
    AbstractParallelTCPSource<EntityRecordLimitedAttrs> source =
            new SQBenchParallelTCPSourceNoExtraAttrs(hostname, port, watermarkInterval);
    return env.addSource(source)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
            // TODO: not used at the moment (and also doesn't make a performance difference as far as I tested it)
//        .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("SQBenchParallelTCPSourceGID" + groupId);
            //.slotSharingGroup("slotGroup" + groupId%4);
  }

  // TODO: I add this method just for compatibility. It should be removed and all calls should be to the method above instead
  // to allow for queryId to be passed
  public static DataStream<EntityRecordFull> newSourceStream(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int watermarkInterval,
          int parallelism) {
    return newSourceStream(env, hostname, port, watermarkInterval, parallelism, 0);
  }

  public static DataStream<EntityRecordFull> entityA(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).name("EntityA");
  }

  public static DataStream<EntityRecordFull> entityB(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).name("EntityB");
  }

  public static DataStream<EntityRecordFull> entityA(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).setParallelism(parallelism).name("EntityA");
  }

  public static DataStream<EntityRecordFull> entityB(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).setParallelism(parallelism).name("EntityB");
  }

  public static DataStream<EntityRecordFull> entityA(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).name("EntityAGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordFull> entityB(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).name("EntityBGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordLimitedAttrs> entityAJoin(int groupId, DataStream<EntityRecordLimitedAttrs> source, int parallelism) {
    return source.filter(e -> e.isOfTypeA).name("EntityAGID" + groupId).setParallelism(parallelism);//.slotSharingGroup("slotGroup" + groupId%4);
  }

  public static DataStream<EntityRecordLimitedAttrs> entityBJoin(int groupId, DataStream<EntityRecordLimitedAttrs> source, int parallelism) {
    return source.filter(e -> !e.isOfTypeA).name("EntityBGID" + groupId).setParallelism(parallelism);//.slotSharingGroup("slotGroup" + groupId%4);
  }

  public static DataStream<EntityRecordFull> entityC(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).name("EntityC");
  }

  public static DataStream<EntityRecordFull> entityC(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).setParallelism(parallelism).name("EntityC");
  }

  public static DataStream<EntityRecordFull> entityC(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).name("EntityCGID" + groupId).setParallelism(parallelism);
  }

  public static final class JSONConfig {
    public static List<QueryGroup<Object>> buildQueries(StreamExecutionEnvironment env, List<String> driverInformation, List<?> queries, int gidOffset, int numTotalQueries, Class<?> clazz) throws Exception {
      List<QueryGroup<Object>> result = new LinkedList<>();

      for (int i = 0; i < queries.size(); i++) {
        String hostPort = driverInformation.get(i);
        String hostname = hostPort.split(":")[0];
        int port = Integer.parseInt(hostPort.split(":")[1]);
        LOGGER.info(
                "Processing data analytics query group " + i + "(driver hostname " + hostname + ", driver port: " + port);

        // TODO: number of queries per query type (data analytics streaming etl) or overall? at the moment, it's the FORMER
        int downstreamParallelismFactor = 1;
        if (clazz == DataAnalyticsQuery.class) {
          if ((Operator) ((DataAnalyticsQuery)queries.get(i)).downstreamOperator != null) {
            downstreamParallelismFactor = ((Operator) ((DataAnalyticsQuery)queries.get(i)).downstreamOperator).parallelismFactor;
          }
          //var q = (ConfigurableDataAnalyticsQueryAggregation) SQBenchQueryFactory.getQuery("CONFIGURABLE_DATA_ANALYTICS_QUERY", env, hostname, port, i + gidOffset, numTotalQueries, env.getParallelism() * ((DataAnalyticsQuery) queries.get(i)).aggregationOperator.parallelismFactor, env.getParallelism() * downstreamParallelismFactor);
          //var q = (ConfigurableDataAnalyticsQueryCoProcess) SQBenchQueryFactory.getQuery("CONFIGURABLE_DATA_ANALYTICS_QUERY", env, hostname, port, i + gidOffset, numTotalQueries, env.getParallelism() * ((DataAnalyticsQuery) queries.get(i)).joinOperators.parallelismFactor, env.getParallelism() * downstreamParallelismFactor);
          var q = (ConfigurableDataAnalyticsQuery) SQBenchQueryFactory.getQuery("CONFIGURABLE_DATA_ANALYTICS_QUERY", env, hostname, port, i + gidOffset, numTotalQueries, env.getParallelism() * ((DataAnalyticsQuery) queries.get(i)).joinOperators.parallelismFactor, env.getParallelism() * downstreamParallelismFactor);
          q.setQueryConfig((DataAnalyticsQuery) queries.get(i), (List<DataAnalyticsQuery>) queries, gidOffset);
          result.add(q);
        } else if (clazz == StreamingETLQuery.class) {
          if ((Operator) ((StreamingETLQuery)queries.get(i)).downstreamOperator != null) {
            downstreamParallelismFactor = ((Operator) ((StreamingETLQuery)queries.get(i)).downstreamOperator).parallelismFactor;
          }
          var q = (ConfigurableStreamingETLQuery) SQBenchQueryFactory.getQuery("CONFIGURABLE_STREAMING_ETL_QUERY", env, hostname, port, i + gidOffset, numTotalQueries, env.getParallelism() * ((StreamingETLQuery) queries.get(i)).extractOperation.parallelismFactor, env.getParallelism() * downstreamParallelismFactor);
          q.setQueryConfig((StreamingETLQuery) queries.get(i), (List<StreamingETLQuery>) queries, gidOffset);
          result.add(q);
        } else {
          throw new IllegalStateException("Unknown query class: " + clazz);
        }
      }

      return result;
    }
  }
}
