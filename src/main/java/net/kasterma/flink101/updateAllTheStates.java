package net.kasterma.flink101;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

@Slf4j
class TwosThrees extends RichSourceFunction<Integer> {
    private Integer last = 2;
    private Boolean running = true;
    private final Long sleep;

    TwosThrees(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            last = last == 2 ? 3 : 2;
            sourceContext.collect(last);
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Data
class Message {
    private Boolean resetState = false;
    private Integer value = 0;
}

@Slf4j
class BroadCastSource extends RichSourceFunction<Message> {
    private final Long sleep;
    private final Long forgetIterations;
    private Long iteration = 0L;
    private Boolean running = true;
    private final Random random = new Random();

    BroadCastSource(Long sleep, Long forgetIterations) {
        super();
        this.sleep = sleep;
        this.forgetIterations = forgetIterations;
    }

    @Override
    public void run(SourceContext<Message> sourceContext) throws Exception {
        while (running) {
            Message m = new Message();
            log.info("it {} fi {}", iteration, forgetIterations);
            if (iteration.equals(forgetIterations)) {
                // emit forget state event
                iteration = 0L;
                log.info("resetstaet");
                m.setResetState(true);
            } else {
                log.info("blalbalbalbalsdjfkaljfahdgajklfdj");
                iteration++;
                // emit update model event
                m.setValue(random.nextInt(5) + 1);
            }
            sourceContext.collect(m);
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Slf4j
class AggregateMultipliedSums extends KeyedBroadcastProcessFunction<Integer, Integer, Message, Integer> {
    private int mulby = 2; // operator state   (broadcast)

    // TODO: set this state to zero (the customer profile)
    private ValueState<Integer> currentSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        currentSum = getRuntimeContext()
                .getState(new ValueStateDescriptor<Integer>("current-sum",
                        TypeInformation.of(new TypeHint<Integer>() {})));
    }

    @Override
    public void processElement(Integer event, KeyedReadOnlyContext keyedReadOnlyContext, Collector<Integer> collector) throws Exception {
        int currentVal = currentSum.value() == null ? 0 : currentSum.value();
        currentVal += event * mulby;
        collector.collect(currentVal);
        currentSum.update(currentVal);
    }

    class FFS extends KeyedStateFunction<Integer, ValueState<Integer>> {

        @Override
        public void process(Integer integer, ValueState<Integer> integerValueState) throws Exception {
            integerValueState.update(0);
        }
    }

    @Override
    public void processBroadcastElement(Message m, KeyedContext keyedContext, Collector<Integer> collector) throws Exception {
        if (m.getResetState()){
            keyedContext.applyToKeyedState(new ValueStateDescriptor<Integer>("current-sum",
                    TypeInformation.of(new TypeHint<Integer>() {})), new FFS());  //currentSum.update(0);
            log.info("zero");
            mulby = 0;
        } else {
            mulby = m.getValue();
            log.info("mullby now: {}", mulby);
        }
    }
}

@Slf4j
class mapFn2 extends RichCoMapFunction<Integer, Message, Integer> {
    int mulby = 2;

    @Override
    public Integer map1(Integer integer) throws Exception {
        return integer * mulby;
    }

    @Override
    public Integer map2(Message param) throws Exception {
        log.info("update param");
        if (!param.getResetState()) {
            mulby = param.getValue();
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
public class updateAllTheStates {
    static KeySelector<Integer, Integer> ks = (i) -> i;

    public static void main(String[] args) throws Exception {
        log.info("started");

        final val env = StreamExecutionEnvironment.getExecutionEnvironment();

        // stream of ordinary events (think pageviews)
        DataStream<Integer> events = env.addSource(new TwosThrees(1000L)).keyBy((i) -> i);

        // stream of update model and reinitialize state events
        BroadcastStream<Message> broadcastEvents = env.addSource(new BroadCastSource(10_000L, 3L))
                .broadcast(new MapStateDescriptor<>("name", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

        val outEvents = events.connect(broadcastEvents).process(new AggregateMultipliedSums());  // combine in the broadcast

        outEvents.print();

        env.execute("show me the numbers");
    }
}
