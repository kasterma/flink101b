package net.kasterma.flink101;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;
import java.util.List;

@Slf4j
class SumIntPairValues implements AggregateFunction<IntPair, List<Integer>, List<Integer>> {

    @Override
    public List<Integer> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Integer> add(IntPair value, List<Integer> accumulator) {
        log.info("add");
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

class Above50 extends Trigger<IntPair, GlobalWindow> {
    @Override
    public TriggerResult onElement(IntPair element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
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
public class ReduceFold {

    public static void main(String[] args) throws Exception {
        final val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        KeyedStream<IntPair, Integer> src =
                env.addSource(new ForeverRandom(1000L))
                        .map(IntPair::new)
                        .setParallelism(5)
                        .keyBy(x -> x.getX() % 2);

        WindowedStream<IntPair, Integer, GlobalWindow> tt = src.window(GlobalWindows.create());
        tt.trigger(new Above50()).aggregate(new SumIntPairValues()).print();
        //WindowedStream<IntPair, Integer, TimeWindow> tt = src.timeWindow(Time.seconds(30));
        //tt.aggregate(new SumIntPairValues()).print();

        env.execute("Flink Streaming Experiment 1");
    }
}
