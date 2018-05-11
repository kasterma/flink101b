package net.kasterma.flink101;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


@Slf4j
class NumberPerSec extends RichSourceFunction<Integer> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep = 1000L;

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

@Slf4j
public class KeyTest {
    public static int keyAndLogit(int p) {
        int key = p % 2;
        log.info("keyAndLogit({})={}", p, key);
        log.info("ss: {}\n{}", Thread.currentThread().getName(),
                Thread.currentThread().getStackTrace());
//        List<Integer> rnds = new ArrayList<>();
//        for(int i = 0; i < 10000; i++){
//            rnds.add(new Random().nextInt(100));
//        }
//        int sum = rnds.stream().reduce(Integer::sum).orElse(0);
//        log.info("sum", sum); // just to make it look used
        return key;
    }

    public static void main(String[] args) throws Exception {
        final val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> src =
                env.addSource(new NumberPerSec())    // task 1
                        //.disableChaining()
                        .keyBy(KeyTest::keyAndLogit)
                .sum(0);                 // task 2
        src//.disableChaining()
                .print().name("printyprintprint");
        env.execute("Flink Streaming Experiment 666");
    }
}
