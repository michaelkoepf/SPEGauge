package net.michaelkoepf.spegauge.flink.sdk.source;

import java.io.*;
import java.net.ProtocolException;
import java.net.Socket;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Abstract source function with hooks that have to be overwritten by the implementing class. This class
 * allows the user to control event generation, time stamps and watermarks and provides all
 * functionality to read events from the benchmarking driver.
 *
 * @param <T> event type that should be used within Apache Flink.
 */
public abstract class AbstractParallelTCPSource<T> extends RichParallelSourceFunction<T> {

  @Getter
  private static final Logger LOGGER =
          LoggerFactory.getLogger(AbstractParallelTCPSource.class);
  protected static final Marker GROUP_SHARE = MarkerFactory.getMarker("GroupShare");

  @Getter
  @Setter
  private String delimiter = "\n";
  @Getter
  @Setter
  private int readBufferSize = 8192;

  /**
   * Specifies the time semantics of the source.
   */
  public enum TimeSemantics {
    EVENT_TIME,
    PROCESSING_TIME
  }

  private final TimeSemantics timeSemantics;
  @Getter
  private String hostname;
  @Getter
  private Integer port;
  private Socket socket;
  private BufferedReader socketReader;
  @Getter
  private Writer socketWriter;
  @Getter
  String driverJobId;
  @Getter
  int driverSubTaskIdx;

  @Getter
  private long eventCount = 0;

  @Getter
  private volatile boolean cancelled = false;

  /**
   * Creates a new instance
   *
   * @param hostname      hostname of server
   * @param port          TCP port the server is listening on for incoming client connections
   * @param timeSemantics desired time semantics
   */
  public AbstractParallelTCPSource(String hostname, int port, TimeSemantics timeSemantics) {
    setConnectionData(hostname, port);
    this.timeSemantics = timeSemantics;
  }

  public AbstractParallelTCPSource(String hostname, int port) {
    this(hostname, port, TimeSemantics.EVENT_TIME);
  }

  /**
   * Converts the string given by line to the desired event object, if the string encodes an event.
   *
   * @param record string representation of event object
   * @return event object if record contained an event, null otherwise (e.g., in case of control messages)
   */
  public abstract T getEvent(String record);

  /**
   * Returns the event timestamp of the given event (in event time) if present.
   * This method is only called if the source is configured to use event time.
   *
   * @param event the event
   * @return event timestamp in milliseconds since epoch or null if event time is not used
   */
  public abstract Long getEventTimeStamp(T event);

  /**
   * Returns a watermark if, based on the information in the event, a watermark should be generated, and null otherwise.
   * This method is only called if the source is configured to use event time.
   *
   * @param event the event
   * @return an instance of Watermark or null
   */
  public abstract Watermark getWatermark(T event);

  /**
   * Connects to the driver and waits for the driver hello message.
   *
   * @param parameters The configuration containing the parameters attached to the contract.
   * @throws Exception
   */
  @Override
  public final void open(Configuration parameters) throws Exception {
    super.open(parameters);
    setupConnection();
  }

  @Override
  public final void close() throws Exception {
    LOGGER.info("Close called. Flush output stream and close connection.");
    super.close();
    closeConnection();
  }

  @Override
  public final void run(SourceContext<T> ctx) {
    // some parts in this function are copied from Apache Flink's SocketTextStreamFunction
    // see https://github.com/apache/flink/blob/94b55d1ae61257f21c7bb511660e7497f269abc7/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SocketTextStreamFunction.java
    StringBuilder buffer = new StringBuilder();
    char[] readBuffer = new char[readBufferSize];
    int bytesRead;

    try {
      while (!this.cancelled && (bytesRead = socketReader.read(readBuffer)) != -1) {
        buffer.append(readBuffer, 0, bytesRead);

        int currentDelimiterPos;
        while (buffer.length() >= this.delimiter.length() && (currentDelimiterPos = buffer.indexOf(this.delimiter)) != -1) {
          String record = buffer.substring(0, currentDelimiterPos);

          if (this.delimiter.equals("\n") && record.endsWith("\r")) {
            record = record.substring(0, record.length() - 1);
          }

          T event = getEvent(record);

          // not a regular event (e.g., control message)
          if (event == null) {
            buffer.delete(0, currentDelimiterPos + this.delimiter.length());
            continue;
          }

          // regular event
          Long timestamp;
          if (timeSemantics.equals(TimeSemantics.EVENT_TIME)) {
            timestamp = getEventTimeStamp(event);
            ctx.collectWithTimestamp(event, timestamp);

            Watermark maybeWatermark = getWatermark(event);

            if (maybeWatermark != null) {
              ctx.emitWatermark(maybeWatermark);
            }
          } else {
            ctx.collect(event);
          }

          eventCount++;
          buffer.delete(0, currentDelimiterPos + this.delimiter.length());
        }
      }
    } catch (IOException e) {
      LOGGER.error("An error occurred while communicating with the server: " + e.getMessage());
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public final void cancel() {
    cancelled = true;
  }


  protected final void closeConnection() throws IOException {
    if (socketWriter != null) {
      socketWriter.flush();
      socketWriter.close();
    }

    if (socketReader != null) {
      socketReader.close();
    }

    if (!socket.isClosed()) {
      socket.close();
    }
  }

  protected void setupConnection() throws IOException {
    LOGGER.debug(GROUP_SHARE, "Connecting to host " + hostname + " on port " + port);

    socket = new Socket(hostname, port);
    socket.setTcpNoDelay(true); // source sends only control messages to driver, no need to buffer
    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    socketWriter = new OutputStreamWriter(socket.getOutputStream());

    // TODO: if we need it for some reason, we can send a CLIENT HELLO here

    LOGGER.debug(GROUP_SHARE, "Waiting for DRIVER HELLO message");

    String driverHello = socketReader.readLine();

    if (driverHello.startsWith("DRIVER HELLO ")) {
      String params = driverHello.replaceFirst("^DRIVER HELLO ", "");
      String[] jobId_subtaskIdx = params.split(" ");
      driverJobId = jobId_subtaskIdx[0];
      driverSubTaskIdx = Integer.parseInt(jobId_subtaskIdx[1]);
    } else {
      throw new ProtocolException("Expected DRIVER HELLO message from driver, but got: " + driverHello);
    }

    LOGGER.debug(GROUP_SHARE, "Received DRIVER HELLO AND CONNECTED SUCCESSFULLY to " + hostname + " on port " + port);
  }

  protected final void setConnectionData(String hostname, Integer port) {
    this.hostname = hostname;
    this.port = port;
  }
}
