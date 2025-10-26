package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Creating this query to be able to test the load shedder
 */
public class DummySourceMapQuery extends QueryGroup<EntityRecordFull> {
    @Override
    public List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        DataStream<EntityRecordFull> queryResult =
                QueryUtils.newSourceStream(
                                env, getHostname(), getPort(), getWatermarkInterval(), getSourceParallelism(), getGroupId())
                        //.rebalance()
                        //.map(x -> x).name("DummyMapGID" + getGroupId())
        ;

        return new ArrayList<>() {
            {
                add(queryResult);
            }
        };
    }

    @Override
    public String getName() {
        return "Dummy Query";
    }
}
