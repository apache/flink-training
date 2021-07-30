package org.apache.flink.training.exercises.testing;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSink<OUT> implements SinkFunction<OUT> {

    // must be static
    public static final List VALUES = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(OUT value, Context context) {
        VALUES.add(value);
    }

    public Iterable<OUT> results() {
        return VALUES;
    }

    public void reset() {
        VALUES.clear();
    }
}
