package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.sut.*;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Base64;

public class SQBenchParallelTCPSourceCSV extends SQBenchParallelTCPSource<EntityRecordFull> implements ReconfigurableSource {

    public SQBenchParallelTCPSourceCSV(String hostname, int port) {
        super(hostname, port);
    }

    public SQBenchParallelTCPSourceCSV(String hostname, int port, int watermarkInterval) {
        super(hostname, port, watermarkInterval);
    }

    @Override
    public Long getEventTimeStamp(EntityRecordFull entity) {
        return entity.eventTimeStampMilliSecondsSinceEpoch;
    }

    @Override
    public Watermark getWatermark(EntityRecordFull event) {
        if (watermarkInterval > 0 && ((event.uniqueTupleId - getDriverSubTaskIdx())  % watermarkInterval == 0)) {
            if (lastWatermark < event.eventTimeStampMilliSecondsSinceEpoch) {
                lastWatermark = event.eventTimeStampMilliSecondsSinceEpoch;
            } else {
                getLOGGER().warn("Watermark is not monotonically increasing. Last watermark: " + lastWatermark + ", current watermark: " + event.eventTimeStampMilliSecondsSinceEpoch);
            }
            return new Watermark(event.eventTimeStampMilliSecondsSinceEpoch);
        } else {
            return null;
        }
    }

    protected static EntityRecordFull attributesToEntityRecord(EntityRecordFull.Type type, String[] attrs) {
        return new EntityRecordFull(type, Long.parseLong(attrs[1]), Long.parseLong(attrs[2]), Long.parseLong(attrs[3]), Long.parseLong(attrs[4]), Long.parseLong(attrs[5]), Long.parseLong(attrs[6]), Long.parseLong(attrs[7]), Long.parseLong(attrs[8]), Long.parseLong(attrs[9]), attrs[10].isEmpty() ? null : Integer.parseInt(attrs[10]), attrs[11], attrs[12], attrs[13], Base64.getDecoder().decode(attrs[14]), Integer.parseInt(attrs[15]));
    }

    protected EntityRecordFull handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        EntityRecordFull entity;
        if (attrs[0].equals("JSON")) {
            if (attrs.length != 6) {
                throw new RuntimeException("JSON entities must have length 6. Got " + attrs.length + " instead");
            }

            entity = new EntityRecordFull(EntityRecordFull.Type.JSON, Long.parseLong(attrs[1]), Long.parseLong(attrs[2]), Long.parseLong(attrs[3]), attrs[4], Integer.parseInt(attrs[5]));
        } else if (attrs[0].equals("A") || attrs[0].equals("B") || attrs[0].equals("C")) {
            if (attrs.length != 16) {
                throw new RuntimeException("Entities must have length 16. Got " + attrs.length + " instead");
            }

            switch (attrs[0]) {
                case "A":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.A, attrs);
                    break;
                case "B":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.B, attrs);
                    break;
                case "C":
                    entity = attributesToEntityRecord(EntityRecordFull.Type.C, attrs);
                    break;
                default:
                    throw new RuntimeException("Invalid event type " + attrs[0]);
                    // TODO: extend for further entities, if necessary
            }
        } else {
            throw new RuntimeException("Invalid event type " + attrs[0]);
        }

        currentEventId.set(entity.uniqueTupleId);

        return entity;
    }
}
