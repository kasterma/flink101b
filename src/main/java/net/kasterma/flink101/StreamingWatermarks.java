package net.kasterma.flink101;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
class ForeverRandomWT extends RichSourceFunction<Integer> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;
    private long lastMark = 0;

    ForeverRandomWT(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            long current_time = System.currentTimeMillis();
            log.info("current_time: {}", current_time);

            sourceContext.collectWithTimestamp(random.nextInt(100), current_time);

            if (current_time >= lastMark + 2500) {  // when started with sleep = 1000, reliably send every third one a watermark
                log.info("send water mark: {}", current_time);
                lastMark = current_time;
                sourceContext.emitWatermark(new Watermark(current_time));
            }

            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        log.info("cancel source");
        running = false;
    }
}

@Slf4j
class Above50WM extends Trigger<IntPair, GlobalWindow> {
    @Override
    public TriggerResult onElement(IntPair element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        Long now = System.currentTimeMillis();
        log.info("Trigger: current time {}", System.currentTimeMillis());
        log.info("Trigger: current watermark: {}", ctx.getCurrentWatermark());
        Long wm = ctx.getCurrentWatermark();
        log.info("Trigger: triggeredBy {}", element);
        log.info("Trigger: delay {}", now - wm);
        log.info("Trigger ss: {}\n{}", Thread.currentThread().getName(),
                Thread.currentThread().getStackTrace());
        if (element.getY() > 50) {
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

    }
}

@Slf4j
class SumIntPairValuesSlowOdd implements AggregateFunction<IntPair, List<Integer>, List<Integer>> {

    @Override
    public List<Integer> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Integer> add(IntPair value, List<Integer> accumulator) {
        log.info("add {}", value);
        log.info("aggregate ss: {}\n{}", Thread.currentThread().getName(),
                Thread.currentThread().getStackTrace());
        try {
            Thread.sleep(6500L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (value.getX() % 2 == 1) {
//            try {
//                Thread.sleep(10000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
        accumulator.add(value.getY());
        return accumulator;
    }

    @Override
    public List<Integer> getResult(List<Integer> accumulator) {
        return accumulator;
    }

    @Override
    public List<Integer> merge(List<Integer> a, List<Integer> b) {
        a.addAll(b);
        return a;
    }
}

@Slf4j
public class StreamingWatermarks {

    public static void main(String[] args) throws Exception {
        final val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(6);

        KeyedStream<IntPair, Integer> src =
                env.addSource(new ForeverRandomWT(1000L))
                        .map(x -> new IntPair(x))
                        .setParallelism(5)
                        .keyBy(x -> x.getX() % 2);

        WindowedStream<IntPair, Integer, GlobalWindow> tt = src.window(GlobalWindows.create());
        tt.trigger(new Above50WM()).aggregate(new SumIntPairValuesSlowOdd()).disableChaining().print();

        env.execute("Flink Streaming Experiment 1");
    }
}
