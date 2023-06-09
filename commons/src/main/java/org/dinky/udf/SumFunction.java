package org.dinky.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author ZackYoung
 * @since 1.0.0
 */
public class SumFunction extends ScalarFunction {

    public Integer eval(Integer a, Integer b) {
        return a + b;
    }

    public Integer eval(String a, String b) {
        return Integer.valueOf(a) + Integer.valueOf(b);
    }

    public Integer eval(Double... d) {
        double result = 0;
        for (double value : d) {
            result += value;
        }
        return (int) result;
    }
}

