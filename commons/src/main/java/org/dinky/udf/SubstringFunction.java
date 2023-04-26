package org.dinky.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author ZackYoung
 * @since 1.0.0
 */
public class SubstringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        System.out.println("this is java");
        return s.substring(begin, end);
    }
}
