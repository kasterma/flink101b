package net.kasterma.flink101;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scopt.Zero;

import java.util.Random;

@Data
@AllArgsConstructor
class Event {
    final int key;
    final int val;
}

@Data
@AllArgsConstructor
class OutEvent {
    final int key;
    final int val;
}

@Slf4j
class KeyedEvents extends RichSourceFunction<Event> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;

    KeyedEvents(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(new Event(random.nextInt(10), 2));
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Slf4j
class AggregateMultipliedSums extends KeyedBroadcastProcessFunction<Integer, Event, Param, OutEvent> {
    final OutputTag<Integer> keysTag;

    public AggregateMultipliedSums(final OutputTag<Integer> keysTag) {
        this.keysTag = keysTag;
    }

    private int mulby = 2; // operator state   (broadcast)

    // TODO: set this state to zero (the customer profile)
    private ValueState<Integer> currentSum;  // keyed state

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        currentSum = getRuntimeContext()
                .getState(new ValueStateDescriptor<Integer>("current-sum",
                        TypeInformation.of(new TypeHint<Integer>() {})));
    }

    @Override
    public void processElement(Event event, KeyedReadOnlyContext keyedReadOnlyContext, Collector<OutEvent> collector) throws Exception {
        int currentVal = currentSum.value() == null ? 0 : currentSum.value();
        currentVal += event.getVal() * mulby;
        collector.collect(new OutEvent(event.getKey(), currentVal));
        keyedReadOnlyContext.output(keysTag, event.getKey());
        currentSum.update(currentVal);
    }

    @Override
    public void processBroadcastElement(Param param, KeyedContext keyedContext, Collector<OutEvent> collector) throws Exception {
        mulby = param.getVal();
        log.info("mullby now: {}", mulby);
    }
}

class ZeroAllStates {}


@Slf4j
class SlowZeroAllStates extends RichSourceFunction<ZeroAllStates> {
    private final Random random = new Random();
    private Boolean running = true;
    private final Long sleep;

    SlowZeroAllStates(Long sleep) {
        super();
        this.sleep = sleep;
    }

    @Override
    public void run(SourceContext<ZeroAllStates> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(new ZeroAllStates());
            Thread.sleep(sleep);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

@Slf4j
public class updateAllTheStates {
    public static void main(String[] args) throws Exception {
        log.info("started");

        final val env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Integer> keysTag = new OutputTag<Integer>("keys-seen") {};

        val events = env.addSource(new KeyedEvents(1000L));

        BroadcastStream<Param> pararmUpdates = env.addSource(new SlowRandom(5*1000L))
                .map(Param::new)
                .broadcast(new MapStateDescriptor<>("name", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));

        val stateUpdates = env.addSource(new SlowZeroAllStates(10*1000L));

        events.print();
        //pararmUpdates.print();

        val outnos = events.keyBy(x -> x.getKey());
        //val out2 = outnos.connect(pararmUpdates);
        //val out3 = outnos.flatMap(new keymapFnWState());

        val sumsSoFar = outnos.connect(pararmUpdates).process(new AggregateMultipliedSums(keysTag));

        val keyStream = sumsSoFar.getSideOutput(keysTag).map(k -> "keykeykey" + k); // .print();

        val keyupdatestream = stateUpdates.union(keyStream).mapwithstate(fffff);  // TODO: make work

        sumsSoFar.print();

        env.execute("show me the numbers");
    }
}
