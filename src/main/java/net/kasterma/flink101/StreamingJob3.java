package net.kasterma.flink101;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Data
@Slf4j
class IntPair{
    Integer x;
    Integer y;

    IntPair(Integer x) {
        this.x = x;
        this.y = x;
        log.info("init {}", x);
    }

    IntPair(Integer x, Integer y) {
        this.x = x;
        this.y = y;
        log.info("({}, {})", x, y);
    }
}

@Slf4j
public class StreamingJob3 {

    public static void main(String[] args) throws Exception {
        final val env = StreamExecutionEnvironment.getExecutionEnvironment();

        val src = env.addSource(new ForeverRandom(10000L)).map(x -> new IntPair(x)).setParallelism(5);

        val it = src.iterate();
        val subbed = it.map(i -> new IntPair(i.getX(), i.getY() - 1));
        val stGO = subbed.filter(i -> i.getY() > 0).setParallelism(5);
        val lTZ = subbed.filter(i -> (i.getY() <= 0));
        it.closeWith(stGO);

        lTZ.print();

        env.execute("Flink Streaming Experiment 1");
    }
}
