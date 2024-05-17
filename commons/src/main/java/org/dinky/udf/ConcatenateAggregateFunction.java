package org.dinky.udf;

import org.apache.flink.table.functions.AggregateFunction;

public class ConcatenateAggregateFunction extends AggregateFunction<String, ConcatenateAggregateFunction.Accumulator> {


    public static class Accumulator {
        public StringBuilder concatString = new StringBuilder();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator accumulator, String value) {
        if (value != null) {
            accumulator.concatString.append(value);
        }
    }

    public void merge(Accumulator accumulator, Iterable<Accumulator> iterable) {
        for (Accumulator otherAcc : iterable) {
            accumulator.concatString.append(otherAcc.concatString);
        }
    }

    @Override
    public String getValue(Accumulator accumulator) {
        return accumulator.concatString.toString();
    }
}

