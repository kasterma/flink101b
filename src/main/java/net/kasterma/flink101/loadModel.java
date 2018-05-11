package net.kasterma.flink101;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;


@Slf4j
class SlowRandom extends RichSourceFunction<Integer> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;

    SlowRandom(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            Thread.sleep(sleep);
            sourceContext.collect(random.nextInt(10));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Slf4j
class Slow2 extends RichSourceFunction<Integer> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;

    Slow2(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(2);
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Data
@AllArgsConstructor
class Param {
   // boolean param;
    int val;
}

@Slf4j
class mapFn extends RichCoMapFunction<Integer, Param, Integer> {
    int mulby = 2;

    @Override
    public Integer map1(Integer integer) throws Exception {
        return integer * mulby;
    }

    @Override
    public Integer map2(Param param) throws Exception {
        log.info("update param");
        if (new Random().nextFloat() > 0.5) {
            mulby = param.getVal();
            log.info("updated");
        } else {
            log.info("no update");
        }
        return 999;
    }

//    @Override
//    public Integer map(Param in) throws Exception {
//        if (in.isParam()) {
//            return 99999;
//        } else {
//            return in.getVal() * 2;
//        }
//    }
}

@Slf4j
public class loadModel {
    public static void main(String[] args) throws Exception {
        log.info("started");

        final val env = StreamExecutionEnvironment.getExecutionEnvironment();

        val events = env.addSource(new Slow2(1000L));//.map(x -> new Param(false, x));

        val pararmUpdates = env.addSource(new SlowRandom(10*1000L))
                .map(x -> new Param(x))
                .broadcast();

        events.print();
        pararmUpdates.print();

        val outnos = events.connect(pararmUpdates).map(new mapFn());

        outnos.print();
        env.execute("show me the numbers");
    }
}
