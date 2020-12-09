package com.github.luojiash;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class TopNWordCount {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .map(new MapFunction<String, WordWithCount>() {
                    @Override
                    public WordWithCount map(String value) throws Exception {
                        String[] array = value.split("\\s", 2);
                        String word = array[0];
                        Date date = DateUtils.parseDate(array[1], "yyyy-MM-dd HH:mm:ss");

                        return new WordWithCount(word, date, 1);
                    }
                })
                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<WordWithCount>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        WatermarkStrategy.<WordWithCount>forMonotonousTimestamps()
                                .withTimestampAssigner((event, context) -> event.date.getTime())
                );


        DataStream<WordWithCount> aggStream = windowCounts
                .keyBy(value -> value.word)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5)))

                .aggregate(new AggregateFunction<WordWithCount, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(WordWithCount value, Long accumulator) {
                        return accumulator + value.count;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new WindowFunction<Long, WordWithCount, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<WordWithCount> out) throws Exception {
                        out.collect(new WordWithCount(key, new Date(window.getEnd()), input.iterator().next()));
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print("data").setParallelism(1);
        aggStream.print("agg").setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public Date date;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Date date, long count) {
            this.word = word;
            this.date = date;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", date=" + date +
                    ", count=" + count +
                    '}';
        }
    }

}