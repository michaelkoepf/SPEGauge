package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.sut.*;
import net.michaelkoepf.spegauge.api.sut.BitSet;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.*;

public class CoProcessJoin<T extends Tuple>
        extends KeyedCoProcessFunction<Integer, T, T,
        Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>
        implements Serializable, JoinMonitor {

    private final long windowSize;
    private final long windowSlide;
    private transient List<OutputTag<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>> outputTags;
    private final int numOfQueries;

    private transient ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> stream1State;
    private transient ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> stream2State;

    // Merge Phase monitoring
    private transient ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> stream1StateMonitoringMerge;
    private transient ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> stream2StateMonitoringMerge;

    JoinDataDistrMergeStats monitoringMergeStats;
    boolean monitoringMergeEnabled;
    DataRange[] monitoringDataRanges;
    HashMap<Integer, HashSet<Integer>> queriesToDataRanges;
    HashSet<Integer> activeQueries;

    // Split Phase monitoring
    private transient HashMap<Integer, Integer> stream1StateMonitoringSplit; // key -> numberOfTuples
    private transient HashMap<Integer, Integer> stream2StateMonitoringSplit;
    boolean monitoringSplitEnabled;
    DataDistrSplitStats monitoringSplitStats;
    int monitoringSplitFrequencyMS = 10000;
    long lastMonitoringSplitTimeMS = 0;

    public CoProcessJoin(long windowSize, long windowSlide, int numOfQueries,
                         List<OutputTag<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>> outputTags) {
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;

        // the first query receives the results from the main output
        assert numOfQueries -1 == outputTags.size();
        this.numOfQueries = numOfQueries;
        this.outputTags = outputTags;
        this.monitoringMergeStats = new JoinDataDistrMergeStats();
        this.monitoringMergeEnabled = false;
        this.monitoringSplitStats = new DataDistrSplitStats();
        this.monitoringSplitEnabled = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple2<Tuple2<Integer, BitSet>, Long>> descriptor1 =
                new ListStateDescriptor<>("stream1State",
                        TypeInformation.of(new TypeHint<Tuple2<Tuple2<Integer, BitSet>, Long>>() {}));
        stream1State = getRuntimeContext().getListState(descriptor1);

        ListStateDescriptor<Tuple2<Tuple2<Integer, BitSet>, Long>> descriptor2 =
                new ListStateDescriptor<>("stream2State",
                        TypeInformation.of(new TypeHint<Tuple2<Tuple2<Integer, BitSet>, Long>>() {}));
        stream2State = getRuntimeContext().getListState(descriptor2);

        ListStateDescriptor<Tuple2<Tuple2<Integer, BitSet>, Long>> descriptor3 =
                new ListStateDescriptor<>("stream1StateMonMerge",
                        TypeInformation.of(new TypeHint<Tuple2<Tuple2<Integer, BitSet>, Long>>() {}));
        stream1StateMonitoringMerge = getRuntimeContext().getListState(descriptor3);

        ListStateDescriptor<Tuple2<Tuple2<Integer, BitSet>, Long>> descriptor4 =
                new ListStateDescriptor<>("stream2StateMonMerge",
                        TypeInformation.of(new TypeHint<Tuple2<Tuple2<Integer, BitSet>, Long>>() {}));
        stream2StateMonitoringMerge = getRuntimeContext().getListState(descriptor4);

        stream1StateMonitoringSplit = new HashMap<>();

        stream2StateMonitoringSplit = new HashMap<>();

        // Initialize outputTags here
        this.outputTags = new ArrayList<>();
        for (int i = 1; i < numOfQueries; i++) {
            this.outputTags.add(new OutputTag<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>(
                    "output" + i, TypeInformation.of(
                            new TypeHint<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>>() {})));
        }
    }

    @Override
    public void processElement1(T value, Context ctx, Collector<Tuple2<Tuple2<Integer, BitSet>,
            Tuple2<Integer, BitSet>>> out) throws Exception {
        long currentTime = ctx.timestamp();

        registerTimerForEvent(currentTime, ctx);

        Tuple2<Integer, BitSet> tuple = getTuple2(value);

        // Join this element with elements from stream2
        if (monitoringMergeEnabled &&
                monitoringMergeStats.actualSampleSizeA < monitoringMergeStats.targetSampleSizeA) {
            monitoringMergeStats.actualSampleSizeA++;
            stream1StateMonitoringMerge.add(Tuple2.of(tuple, currentTime));
            joinStreamsMonitoringMerge(tuple, currentTime, stream2State, stream2StateMonitoringMerge, out, ctx);
        }
        else {
            if (monitoringSplitEnabled &&
                    monitoringSplitStats.actualSampleSizeA < monitoringSplitStats.targetSampleSizeA) {
                registerTimerForNextMonitoring(ctx);
                monitoringSplitStats.actualSampleSizeA++;
                updateSelectivitiesSplitMonitoring(tuple.f1);
                stream1StateMonitoringSplit.merge(ctx.getCurrentKey(), 1, Integer::sum);
                joinStreamsMonitoringSplit(tuple, stream2StateMonitoringSplit, ctx);
            }
            stream1State.add(Tuple2.of(tuple, currentTime));
            joinStreams(tuple, currentTime, stream2State, stream2StateMonitoringMerge, out, ctx);
        }
    }

    @Override
    public void processElement2(T value, Context ctx, Collector<Tuple2<Tuple2<Integer, BitSet>,
            Tuple2<Integer, BitSet>>> out) throws Exception {
        long currentTime = ctx.timestamp();

        registerTimerForEvent(currentTime, ctx);

        Tuple2<Integer, BitSet> tuple = getTuple2(value);

        // Join this element with elements from stream1
        if (monitoringMergeEnabled &&
                monitoringMergeStats.actualSampleSizeB < monitoringMergeStats.targetSampleSizeB) {
            monitoringMergeStats.actualSampleSizeB++;
            stream2StateMonitoringMerge.add(Tuple2.of(tuple, currentTime));
            joinStreamsMonitoringMerge(tuple, currentTime, stream1State, stream1StateMonitoringMerge, out, ctx);
        }
        else {
            if (monitoringSplitEnabled &&
                    monitoringSplitStats.actualSampleSizeB < monitoringSplitStats.targetSampleSizeB) {
                registerTimerForNextMonitoring(ctx);
                monitoringSplitStats.actualSampleSizeB++;
                updateSelectivitiesSplitMonitoring(tuple.f1);
                stream2StateMonitoringSplit.merge(ctx.getCurrentKey(), 1, Integer::sum);
                joinStreamsMonitoringSplit(tuple, stream1StateMonitoringSplit, ctx);
            }
            stream2State.add(Tuple2.of(tuple, currentTime));
            joinStreams(tuple, currentTime, stream1State, stream1StateMonitoringMerge, out, ctx);
        }
    }

    private Tuple2<Integer, BitSet> getTuple2(T tuple) {
        if (tuple instanceof Tuple2) {
            return (Tuple2<Integer, BitSet>) tuple;
        }
        else if (tuple instanceof Tuple3) {
            Tuple3<Integer, Integer, BitSet> tuple3 = (Tuple3<Integer, Integer, BitSet> ) tuple;
            return new Tuple2<Integer, BitSet>(tuple3.f1, tuple3.f2);
        }
        else {
            throw new IllegalArgumentException("Tuple type not supported");
        }
    }

    private void registerTimerForNextMonitoring(Context ctx) {
        if (monitoringSplitStats.actualSampleSizeA + monitoringSplitStats.actualSampleSizeB == 0) {
            lastMonitoringSplitTimeMS = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(lastMonitoringSplitTimeMS + monitoringSplitFrequencyMS);
        }
    }

    private void registerTimerForEvent(long eventTime, Context ctx) {
        long startOfSlide = eventTime/windowSlide * windowSlide;

        long tupleExpirationTime = startOfSlide + windowSize;
        ctx.timerService().registerEventTimeTimer(tupleExpirationTime);
    }

    private void joinStreams(
            Tuple2<Integer, BitSet> value, long currentTime, ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> otherStreamState,
            ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> otherStreamStateMonitoringMerge,
            Collector<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> out, Context ctx) throws Exception {
        for (Tuple2<Tuple2<Integer, BitSet>, Long> entry : otherStreamState.get()) {
            if (currentTime - entry.f1 <= windowSize) {
                outputResult(value, entry.f0, ctx, out);
            }
        }
        for (Tuple2<Tuple2<Integer, BitSet>, Long> entry : otherStreamStateMonitoringMerge.get()) {
            if (currentTime - entry.f1 <= windowSize) {
                DataRange dataRange = getDataRange(entry.f0.f1);
                outputResultOneTupleIsMergeMonitoring(value, entry.f0, dataRange, ctx, out);
            }
        }
    }

    private void joinStreamsMonitoringSplit(
            Tuple2<Integer, BitSet> value,
            HashMap<Integer, Integer> otherStreamStateMonitoringSplit, Context ctx) {
        if (otherStreamStateMonitoringSplit.containsKey(ctx.getCurrentKey())) {
            int numOfTuplesInOtherStream = otherStreamStateMonitoringSplit.get(ctx.getCurrentKey());
            incrementJoinMatchesSplitMonitoring(value.f1, numOfTuplesInOtherStream);
        }
    }

    private void joinStreamsMonitoringMerge(
            Tuple2<Integer, BitSet> value, long currentTime, ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> otherStreamState,
            ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> otherStreamStateMonitoringMerge,
            Collector<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> out, Context ctx) throws Exception {
        DataRange dataRange1 = getDataRange(value.f1);
        incrementTotalElementsMergeMonitoring(dataRange1);
        for (Tuple2<Tuple2<Integer, BitSet>, Long> entry : otherStreamStateMonitoringMerge.get()) {
            if (currentTime - entry.f1 <= windowSize) {
                Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>> result = join(value, entry.f0);

                DataRange dataRange2 = getDataRange(entry.f0.f1);
                incrementJoinMatchesMergeMonitoring(dataRange1, dataRange2);

                for (Map.Entry<Integer, HashSet<Integer>> queryWithRanges : queriesToDataRanges.entrySet()) {
                    int query = queryWithRanges.getKey();
                    if (!activeQueries.contains(query)) {
                        continue;
                    }
                    HashSet<Integer> dataRanges = queryWithRanges.getValue();
                    if (dataRanges.contains(dataRange1) && dataRanges.contains(dataRange2)) {
                        if (query == 0) {
                            out.collect(result);
                        }
                        else {
                            ctx.output(outputTags.get(query-1), result);
                        }
                    }
                }
            }
        }
        for (Tuple2<Tuple2<Integer, BitSet>, Long> entry : otherStreamState.get()) {
            if (currentTime - entry.f1 <= windowSize) {
                outputResultOneTupleIsMergeMonitoring(entry.f0, value, dataRange1, ctx, out);
            }
        }
    }

    private Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>> join(
            Tuple2<Integer, BitSet> element1, Tuple2<Integer, BitSet> element2) {
        return new Tuple2<>(element1, element2);
    }

    private void outputResult(
            Tuple2<Integer, BitSet> tuple1, Tuple2<Integer, BitSet> tuple2, Context ctx,
            Collector<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> out){
        Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>> result = join(tuple1, tuple2);
        for (int i = 0; i < numOfQueries; i++) {
            if (tuple1.f1.get(i) && tuple2.f1.get(i)) {
                if (i == 0) {
                    out.collect(result);
                }
                else {
                    ctx.output(outputTags.get(i-1), result);
                }
            }
        }
    }

    private void outputResultOneTupleIsMergeMonitoring(
            Tuple2<Integer, BitSet> tuple1, Tuple2<Integer, BitSet> tuple2MergeMonitoring,
            DataRange dataRange, Context ctx,
            Collector<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>> out){
        Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>> result = join(tuple1, tuple2MergeMonitoring);
        for (Map.Entry<Integer, HashSet<Integer>> queryWithRanges : queriesToDataRanges.entrySet()) {
            int query = queryWithRanges.getKey();
            if (!activeQueries.contains(query)) {
                continue;
            }
            HashSet<Integer> dataRanges = queryWithRanges.getValue();
            if (dataRanges.contains(dataRange) && tuple1.f1.get(query)) {
                if (query == 0) {
                    out.collect(result);
                }
                else {
                    ctx.output(outputTags.get(query-1), result);
                }
            }
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Tuple2<Integer, BitSet>,
            Tuple2<Integer, BitSet>>> out) throws Exception {
        if (monitoringSplitEnabled && ctx.timerService().currentProcessingTime()  - lastMonitoringSplitTimeMS >= monitoringSplitFrequencyMS) {
            System.out.println("Timer! Split stats: " + monitoringSplitStats.toString() + " from time: "
                    + lastMonitoringSplitTimeMS
                    + " stream1: " + stream1StateMonitoringSplit + " stream2: " + stream2StateMonitoringSplit);
            stream1StateMonitoringSplit.clear();
            stream2StateMonitoringSplit.clear();
            monitoringSplitStats.clear();
        }
        long currentTime = ctx.timestamp();

        // timer for expiring tuples
        //if (currentTime % windowSlide != 0) {
            // Remove expired elements from stream1State
            removeExpiredElements(stream1State, currentTime);
            // Remove expired elements from stream2State
            removeExpiredElements(stream2State, currentTime);

            removeExpiredElements(stream1StateMonitoringMerge, currentTime);
            removeExpiredElements(stream2StateMonitoringMerge, currentTime);
        /*}
        // timer for outputting window results
        else {
            // for each element in stream1State, join with elements in stream2State
            for (Tuple2<EntityRecord, Long> entry : stream1State.get()) {
                joinStreams(entry.f0, entry.f1, stream2State, out, ctx);
            }
        }*/
    }

    private void removeExpiredElements(ListState<Tuple2<Tuple2<Integer, BitSet>, Long>> state, long currentTime)
            throws Exception {
        List<Tuple2<Tuple2<Integer, BitSet>, Long>> allElements = new ArrayList<>();
        long minTimestamp = currentTime - windowSize + windowSlide;
        for (Tuple2<Tuple2<Integer, BitSet>, Long> entry : state.get()) {
            if (entry.f1 >= minTimestamp) {
                allElements.add(entry);
            }
        }
        state.update(allElements);
    }

    private DataRange getDataRange(BitSet bitSet) {
        for (int i = 0; i < monitoringDataRanges.length; i++) {
            if (bitSet.get(i)) {
                return monitoringDataRanges[i];
            }
        }
        throw new IllegalStateException("No data range found for tuple");
    }

    private void incrementTotalElementsMergeMonitoring(DataRange dataRange) {
        monitoringMergeStats.totalNumElementsPerDataRange.merge(dataRange, 1L, Long::sum);
    }

    private void incrementJoinMatchesMergeMonitoring(DataRange range1, DataRange range2) {
        AbstractMap.SimpleImmutableEntry<DataRange, DataRange> pair;
        if (range1.start < range2.start) {
            pair = new AbstractMap.SimpleImmutableEntry<>(range1, range2);
        }
        else {
            pair = new AbstractMap.SimpleImmutableEntry<>(range2, range1);
        }
        monitoringMergeStats.joinMatchesPerDataRangePair.merge(pair, 1L, Long::sum);
    }

    private void updateSelectivitiesSplitMonitoring(BitSet querySet) {
        for (int i = 0; i < querySet.size(); i++) {
            if (querySet.get(i)) {
                monitoringSplitStats.selectedNumElementsPerQuery.merge(i, 1, Integer::sum);
            }
        }
    }

    private void incrementJoinMatchesSplitMonitoring(BitSet querySet, int numOfMatches) {
        for (int i = 0; i < querySet.size(); i++) {
            if (querySet.get(i)) {
                monitoringSplitStats.joinMatchesPerQuery.merge(i, numOfMatches, Integer::sum);
            }
        }
    }

    @Override
    public void disableMonitoringMerge() {
        assert monitoringMergeEnabled : "Monitoring not enabled";
        //System.out.println("Disable merge monitoring. " + monitoringMergeEnabled + " " +
                //monitoringMergeStats.actualSampleSizeA + " " + monitoringMergeStats.actualSampleSizeB);
        this.monitoringMergeEnabled = false;
    }

    @Override
    public JoinDataDistrMergeStats getJoinDataDistrMergeStats() {
        assert !monitoringMergeEnabled : "Monitoring still enabled";
        //System.out.println("Join merge stats: " + monitoringMergeStats.toString() + " enabled: " + monitoringMergeEnabled);
        return monitoringMergeStats;
    }

    @Override
    public void enableMonitoringMerge(
            HashMap<Integer, HashSet<Integer>> activeQueriesToDataRanges, DataRange[] dataRanges,
            HashSet<Integer> activeQueries) {
        assert !monitoringMergeEnabled;
        //System.out.println("Enable merge monitoring " + monitoringMergeEnabled + " " +
                //monitoringMergeStats.actualSampleSizeA + " " + monitoringMergeStats.actualSampleSizeB);
        this.monitoringMergeEnabled = true;
        this.monitoringMergeStats.clear();
        this.monitoringDataRanges = dataRanges;
        this.queriesToDataRanges = activeQueriesToDataRanges;
        this.activeQueries = activeQueries;
    }

    @Override
    public void enableMonitoringSplit() {
        monitoringSplitStats.clear();
        //System.out.println("Enable split monitoring");
        monitoringSplitEnabled = true;
    }

     @Override
    public void disableMonitoringSplit() {
        monitoringSplitEnabled = false;
        //System.out.println("Disable split monitoring. Split stats: " + monitoringSplitStats.toString());
        monitoringSplitStats.clear();
    }

    @Override
    public DataDistrSplitStats getSplitMonitoringStats() {
        assert monitoringSplitEnabled : "Monitoring not enabled";
        System.out.println("Join split stats: " + monitoringSplitStats.toString());
        return monitoringSplitStats;
    }
}

