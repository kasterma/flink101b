package net.kasterma.flink101;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

class ForeveryOnes extends RichSourceFunction<Integer> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(1);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

class Ble implements SourceFunction<Integer> {

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}

@Slf4j
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Integer[] xs = {1, 2, 3, 4, 5};
        List<Integer> xsl = Arrays.asList(xs);

        val src1 = env.fromCollection(xsl);

        val src = env.addSource(new ForeveryOnes());

        src.print();

        env.execute("Flink Streaming Experiment 1");
    }
}
