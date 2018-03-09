package net.kasterma.flink101;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class StreamingJob4 {

    public static void main(String[] args) throws Exception {
        final val env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        KeyedStream<IntPair, Integer> src =
                env.addSource(new ForeverRandom(10000L))
                        .map(x -> new IntPair(x))
                        .setParallelism(5)
                        .keyBy(x -> x.getX() % 2);

        IterativeStream<IntPair> it = src.iterate();
        DataStream<IntPair> subbed = it.map(i -> new IntPair(i.getX(), i.getY() - 1));
        log.info("bkbkbk par {}", subbed.getParallelism());
        DataStream<IntPair> stGO = subbed.filter(i -> i.getY() > 0).setParallelism(5);
        DataStream<IntPair> lTZ = subbed.filter(i -> (i.getY() <= 0));
        it.closeWith(stGO);

        lTZ.print();

        env.execute("Flink Streaming Experiment 1");
    }
}
