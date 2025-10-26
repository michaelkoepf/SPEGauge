package net.michaelkoepf.spegauge.flink.sdk.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DefaultSink<T> extends RichSinkFunction<T> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        value = null;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
