package org.dinky.udf;

import java.util.HashMap;
import java.util.Map;

public class FactorAvgAccumulator {
    // 记录每一个factor的ratio之和
    public Map<String, Double> factorRatioMap = new HashMap<>();

    // 记录factor的数量
    public int factorCount = 0;
}
