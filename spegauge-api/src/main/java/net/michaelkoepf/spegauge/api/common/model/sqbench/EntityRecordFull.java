package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.util.Base64;

import net.michaelkoepf.spegauge.api.common.ObservableEvent;
import net.michaelkoepf.spegauge.api.sut.BitSet;

public class EntityRecordFull extends EntityRecordWithQuerySet implements ObservableEvent {

    private String serializedEntity;

    public enum Type {
        // TODO: you can add further entities here
        A("A"),
        B("B"),
        C("C"),
        JSON("JSON"); // data inside JSON attribute

        public final String value;

        Type(String value) {
            this.value = value;
        }
    }

    public Type type;
    public long uniqueTupleId;
    public long eventTimeStampMilliSecondsSinceEpoch;
    public long wallClockTimeStampMilliSecondsSinceEpoch;
    public long PK;
    public long FK;
    public long selectivityAttribute1;
    public long selectivityAttribute2;

    public long longAttribute1;
    public long longAttribute2;

    public String plainString;
    public String jsonString;
    public String xmlString;
    public byte[] padding;

    public EntityRecordFull() {
    }

    public EntityRecordFull(Type type, long uniqueTupleId, long eventTimeStampMilliSecondsSinceEpoch,
                            long wallClockTimeStampMilliSecondsSinceEpoch, String jsonString, int numOfQueries) {
        this.type = type;
        this.uniqueTupleId = uniqueTupleId;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.wallClockTimeStampMilliSecondsSinceEpoch = wallClockTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.querySet = new BitSet(numOfQueries);

        if (type == Type.JSON) {
            this.serializedEntity =  type.value
                    + "\t"
                    + uniqueTupleId
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + wallClockTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + jsonString
                    + "\t"
                    + querySet.size();
        } else {
            throw new IllegalArgumentException("Type must be JSON");
        }
    }

    public EntityRecordFull(Type type, long uniqueTupleId, long eventTimeStampMilliSecondsSinceEpoch,
                            long wallClockTimeStampMilliSecondsSinceEpoch, long PK, long FK, long selectivityAttribute1,
                            long selectivityAttribute2, long longAttribute1, long longAttribute2, int filterAttribute,
                            String plainString, String jsonString, String xmlString, byte[] padding, int numOfQueries) {
        this.type = type;
        this.uniqueTupleId = uniqueTupleId;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.wallClockTimeStampMilliSecondsSinceEpoch = wallClockTimeStampMilliSecondsSinceEpoch;
        this.PK = PK;
        this.FK = FK;
        this.selectivityAttribute1 = selectivityAttribute1;
        this.selectivityAttribute2 = selectivityAttribute2;
        this.longAttribute1 = longAttribute1;
        this.longAttribute2 = longAttribute2;
        this.filterAttribute = filterAttribute;
        this.plainString = plainString;
        this.jsonString = jsonString;
        this.xmlString = xmlString;
        this.padding = padding;
        this.querySet = new BitSet(numOfQueries);

        if (type == Type.A || type == Type.B || type == Type.C) {
            this.serializedEntity =  type.value
                    + "\t"
                    + uniqueTupleId
                    + "\t"
                    + eventTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + wallClockTimeStampMilliSecondsSinceEpoch
                    + "\t"
                    + PK
                    + "\t"
                    + FK
                    + "\t"
                    + selectivityAttribute1
                    + "\t"
                    + selectivityAttribute2
                    + "\t"
                    + longAttribute1
                    + "\t"
                    + longAttribute2
                    + "\t"
                    + filterAttribute
                    + "\t"
                    + (plainString == null ? "" : plainString)
                    + "\t"
                    + (jsonString == null ? "" : jsonString)
                    + "\t"
                    + (xmlString == null ? "" : xmlString)
                    + "\t"
                    // introduces ~1/3 overhead
                    + Base64.getEncoder().encodeToString(padding)
                    + "\t"
                    + querySet.size();
        } else {
            throw new IllegalArgumentException("Type must be A, B or C");
        }
    }


    public String toStringTSV() {
        return serializedEntity;
    }

    @Override
    public long getEventEmissionTimeStampMilliSeconds() {
        return wallClockTimeStampMilliSecondsSinceEpoch;
    }
}
