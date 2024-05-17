package org.dinky.udf;

import java.util.HashMap;
import java.util.Map;

/**
 * sentiment_stat字段的值示例：
 * [{"name": "正向", "count": 1, "ratio": 0.2},
 * {"name": "中性", "count": 4, "ratio": 0.8},
 * {"name": "负向", "count": 0, "ratio": 0.0}]
 */
public class SentimentStatAvgAccumulator {
    // 结果集的正向、中性、负向的ratio之和的map，对应sentiment_stat字段的ratio
    // public Map<String, Double> sentimentRatioMap = new HashMap<>();

    // 聚合的结果条数
    // public int recordNum = 0;

    // 舆情信息的条数，对应sentiment_stat字段的count之和
    public Map<String, Integer> sentimentCount = new HashMap<>();
}
