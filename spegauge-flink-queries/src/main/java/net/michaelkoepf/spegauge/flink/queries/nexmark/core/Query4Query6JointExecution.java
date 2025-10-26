package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.nexmark.core.function.AvgPricePerCategory;
import net.michaelkoepf.spegauge.flink.queries.nexmark.core.function.AvgPriceTop10AuctionPerSeller;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Query4Query6JointExecution extends QueryGroup<Map<Long, Double>> {

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env)
      throws Exception {
    /*
     * Query
     */
    DataStream<Event> source =
        QueryUtils.newSourceStream(
                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism())
            .rebalance();

    // shared part
    DataStream<Event> auctions = QueryUtils.filterAuctions(source, getParallelism() * 2);

    DataStream<Event> bids = QueryUtils.filterBids(source, getParallelism() * 2);

    DataStream<Tuple4<Long, Long, Long, Long>> winningBids =
        QueryUtils.getWinningBids(auctions, bids, getParallelism() * 2);

    // individual parts
    DataStream<Map<Long, Double>> query4Result =
        winningBids
            .keyBy(x -> x.f2) // keyBy category id
            .window(SlidingEventTimeWindows.of(getWindowSize(), getWindowSlide()))
            .aggregate(new AvgPricePerCategory())
            .returns(TypeInformation.of(new TypeHint<Map<Long, Double>>() {}));

    DataStream<Map<Long, Double>> query6Result =
        winningBids
            .keyBy(x -> x.f3) // keyBy seller
            .window(SlidingEventTimeWindows.of(getWindowSize(), getWindowSlide()))
            .aggregate(new AvgPriceTop10AuctionPerSeller())
            .returns(TypeInformation.of(new TypeHint<Map<Long, Double>>() {}));

    return new LinkedList<DataStream>() {
      {
        add(query4Result);
        add(query6Result);
      }
    };
  }

  @Override
  public String getName() {
    return "Nexmark Query4 + Query6 -- Joint Execution";
  }
}
