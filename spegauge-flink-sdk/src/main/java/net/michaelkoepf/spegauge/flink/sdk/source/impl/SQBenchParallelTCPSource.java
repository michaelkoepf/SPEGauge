package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.sut.*;
import net.michaelkoepf.spegauge.flink.sdk.source.AbstractParallelTCPSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SQBenchParallelTCPSource<T> extends AbstractParallelTCPSource<T> implements ReconfigurableSource {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final SuspendableLockObject inactivityLock = new SuspendableLockObject();

    private State state = State.ACTIVE;

    protected final AtomicLong currentEventId = new AtomicLong(-1L);

    private Long expectedLastEvent = null;

    private AtomicReference<StreamTask> streamTaskPointer = new AtomicReference<>(null);
    private AtomicReference<ReconfigurableSourceData> reconfigurableSourceDataAtomicReference = new AtomicReference<>(null);
    protected int watermarkInterval = -1;

    protected long lastWatermark = 0;

    public SQBenchParallelTCPSource(String hostname, int port) {
        super(hostname, port);
    }

    public SQBenchParallelTCPSource(String hostname, int port, int watermarkInterval) {
        super(hostname, port);
        this.watermarkInterval = watermarkInterval;
    }

    @Override
    public T getEvent(String record) {
        if (record.trim().equals("LAST EVENT ACK " + getDriverSubTaskIdx() + " " + expectedLastEvent)) {
            handleLastEventAck();
            return null;
        } else if (record.startsWith("LAST EVENT BEFORE COPY REQ " + getDriverSubTaskIdx() + " ")) {
            getLOGGER().debug(GROUP_SHARE, "Received LAST EVENT BEFORE COPY REQ");
            // TODO: set some data to inform job manager what the last event was; send back ack?
            // call method in StreamTask using reflection to avoid cyclic dependency
            try {
                StreamTask.class.getMethod("sendPendingControlMessgaeDownstream").invoke(streamTaskPointer.get());
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                getLOGGER().error("An error occurred while invoking sendPendingControlMessgaeDownstreame: " + e.getMessage());
                throw new RuntimeException(e);
            }
            return null;
        } else {
            return handleEvent(record);
        }
    }

    @Override
    public abstract Long getEventTimeStamp(T entity);

    @Override
    public abstract Watermark getWatermark(T event);

    @Override
    public State getState() {
        try {
            stateLock.lock();
            return state;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public ReconfigurableSourceData getData() {
        return new ReconfigurableSourceData(getHostname(), getPort(), getDriverJobId(), getDriverSubTaskIdx(), currentEventId.get());
    }

    @Override
    public void resume() {
        try {
            stateLock.lock();
            if (state == State.INACTIVE) {
                state = State.RESUME_REQ;
                synchronized (inactivityLock) {
                    inactivityLock.notifyAll();
                }
            } else {
                throw new SuspendableException("Cannot resume source that is not in INACTIVE state");
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void handleLastEventAck() {
        getLOGGER().info("Disconnecting from Driver");

        try {
            super.closeConnection();
        } catch (IOException e) {
            getLOGGER().error("Disconnection from driver failed");
            throw new UncheckedIOException(e);
        }

        try {
            stateLock.lock();
            state = State.INACTIVE;
            super.setConnectionData(null, null);
        } finally {
            stateLock.unlock();
        }

        // suspend thread until RESUME_REQ
        getLOGGER().debug(GROUP_SHARE, "Try to acquire inactivityLock");
        synchronized (inactivityLock) {
            while (true) {
                try {
                    getLOGGER().debug(GROUP_SHARE, "Suspending thread until RESUME_REQ");
                    inactivityLock.wait();

                    // inactivityLock was notified
                    try {
                        stateLock.lock();
                        if (state == State.RESUME_REQ) {
                            if (getHostname() == null || getPort() == null) {
                                getLOGGER().error("Cannot resume source without connection data");
                                throw new SuspendableException("Cannot resume source without connection data");
                            }
                            getLOGGER().debug(GROUP_SHARE, "Connecting to Driver...");
                            try {
                                super.setupConnection();
                            } catch (IOException ex) {
                                getLOGGER().error("Reconnection to driver failed");
                                throw new UncheckedIOException("Reconnection to driver failed", ex);
                            }
                            state = State.ACTIVE;
                            break;
                        } else {
                            throw new SuspendableException("Expected status RESUME_REQ, but got " + state + " instead");
                        }
                    } finally {
                        stateLock.unlock();
                    }
                } catch (InterruptedException e) {
                    // clear the interrupt flag, otherwise we keep triggering InterruptedExceptions upon every wait
                    Thread.interrupted();
                    try {
                        stateLock.lock();

                        // wake up condition
                        if (state != State.INACTIVE) {
                            throw new SuspendableException("Cannot resume source in current state");
                        } else if (isCancelled()) {
                            // cancellation via Flink API requested
                            getLOGGER().info("Cancel requested");
                            break;
                        } else {
                            getLOGGER().info("Spurious wakeup while waiting for RESUME_REQ");
                        }
                    } finally {
                        stateLock.unlock();
                    }
                }
            }
        }
    }

    protected abstract T handleEvent(String record);

    @Override
    public void modifyConnectionData(String host, int port) {
        stateLock.lock();
        try {
            if (state != State.INACTIVE) {
                throw new ReconfigurationException("Cannot modify connection in current state.");
            }

            super.setConnectionData(host, port);
        } finally {
            stateLock.unlock();
            resume();
        }
    }

    @Override
    public void requestDisconnectionFromDriver(Map<Integer, ReconfigurableSourceData> data) {
        getLOGGER().debug(GROUP_SHARE, "Requesting disconnection from driver on subtask " + getDriverSubTaskIdx()); //+ " with data " + data);
        if (data.get(getDriverSubTaskIdx()) == null) {
            throw new IllegalArgumentException("Data for driver subtask " + getDriverSubTaskIdx() + " is null" + data);
        }
        long lastEventId = data.get(getDriverSubTaskIdx()).getLastEventId();
        expectedLastEvent = lastEventId;
        getLOGGER().debug(GROUP_SHARE, "Sending LAST EVENT REQ " + getDriverSubTaskIdx() + " " + lastEventId);

        try {
            getSocketWriter().write("LAST EVENT REQ " + getDriverSubTaskIdx() + " " + lastEventId +"\n");
            getSocketWriter().flush();
        } catch (IOException e) {
            getLOGGER().error("An error occurred while sending LAST EVENT REQ: " + e.getMessage());
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setStreamTaskPointer(StreamTask tuple) {
        streamTaskPointer.set(tuple);
    }
}
