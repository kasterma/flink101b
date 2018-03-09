package net.kasterma.flink101;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Int;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Slf4j
class ForeverRandom extends RichSourceFunction<Integer> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;

    ForeverRandom(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(random.nextInt(100));
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        log.info("cancel source");
        running = false;
    }
}

@Data
@AllArgsConstructor
class NamedInt {
    String name;
    Integer i;
}

class AggFn implements AggregateFunction<Integer, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return 10;
    }

    @Override
    public Integer add(Integer value, Integer accumulator) {
        return value + accumulator;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}

class StringAggregate implements AggregateFunction<Integer, List<Integer>, String> {

    @Override
    public List<Integer> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Integer> add(Integer value, List<Integer> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public String getResult(List<Integer> accumulator) {
        return accumulator.stream().map(x -> x.toString()).collect(Collectors.joining(", "));
    }

    @Override
    public List<Integer> merge(List<Integer> a, List<Integer> b) {
        a.addAll(b);
        return a;
    }
}

@Slf4j
public class StreamingJob2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        val src = env.addSource(new ForeverRandom(1000L));
        val sumsum = src.keyBy(x -> x%2).timeWindow(Time.seconds(4)).sum(0);

        val windowed = src.keyBy(x -> x%2).timeWindow(Time.seconds(4));
        val sumsum2 = windowed.aggregate(new AggFn());
        val all = windowed.aggregate(new StringAggregate());

        src.map(x -> new NamedInt("src", x)).print();
        sumsum.map(x -> new NamedInt("sum", x)).print();
        sumsum2.map(x -> new NamedInt("sum2", x)).print();
        all.print();

        env.execute("Flink Streaming Experiment 1");
    }
}
