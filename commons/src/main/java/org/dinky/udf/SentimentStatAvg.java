package org.dinky.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.AggregateFunction;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 *  * sentiment_stat字段的值示例：
 *  * [{"name": "正向", "count": 1, "ratio": 0.2},
 *  * {"name": "中性", "count": 4, "ratio": 0.8},
 *  * {"name": "负向", "count": 0, "ratio": 0.0}]
 */
public class SentimentStatAvg extends AggregateFunction<String, SentimentStatAvgAccumulator> {

    @Override
    public SentimentStatAvgAccumulator createAccumulator() {
        return new SentimentStatAvgAccumulator();
    }

    @Override
    public String getValue(SentimentStatAvgAccumulator acc) {
        return covertMap2Json(acc);
    }

    /**
     * 拿到舆情信息的条数，根据条数重新算占比，因此ratio字段不需要
     * @param acc
     * @param anotherFactor
     */
    public void accumulate(SentimentStatAvgAccumulator acc, String anotherFactor) {
        // 验证是否是默认值
        if (anotherFactor.equals("") || anotherFactor.equals("[]") || anotherFactor.equals("-1") || anotherFactor.startsWith("{")) {
            return;
        }

        // 验证是否是合法json
        JSONArray jsonArray = null;
        try {
            jsonArray = JSON.parseArray(anotherFactor);
        } catch (Exception exception) {
            return;
        }

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String name = obj.getString("name");
            // Double ratio = obj.getDouble("ratio");
            // Double rate = obj.getDouble("rate");

            // 优先使用ratio的值
            // if (ratio == null && rate != null) {
            //     ratio = rate;
            // }
            // 如果ratio/rate都为null，应该是数据异常，此处先不考虑，便于即时抛出错误

            // 累加占比
            // acc.sentimentRatioMap.put(name, acc.sentimentRatioMap.getOrDefault(name, 0.0) + ratio);

            // 舆情信息条数
            Integer count = obj.getInteger("count");
            acc.sentimentCount.put(name, acc.sentimentCount.getOrDefault(name, 0) + count);
        }

        // 聚合结果条数
        // acc.recordNum += 1;
    }

    /**
     * 非必须接口，用于撤销已经计算的中间结果的场景
     * @param acc
     * @param anotherFactor
     */
    public void retract(SentimentStatAvgAccumulator acc, String anotherFactor) {
        JSONArray jsonArray = JSON.parseArray(anotherFactor);

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String name = obj.getString("name");
            // Double ratio = obj.getDouble("ratio");

            // 累加占比
            // acc.sentimentRatioMap.put(name, acc.sentimentRatioMap.getOrDefault(name, 0.0) - ratio);

            // 舆情信息条数
            Integer count = obj.getInteger("count");
            acc.sentimentCount.put(name, acc.sentimentCount.getOrDefault(name, 0) - count);
        }

        // 记录条数
        // acc.recordNum -= 1;
    }

    public void merge(SentimentStatAvgAccumulator acc, Iterable<SentimentStatAvgAccumulator> iterableFactorFactorAvgAcc) {
        for (SentimentStatAvgAccumulator factorItem : iterableFactorFactorAvgAcc) {
            // HashMap合并到acc
            // for (Map.Entry<String, Double> entry : factorItem.sentimentRatioMap.entrySet()) {
            //    acc.sentimentRatioMap.merge(entry.getKey(), entry.getValue(), Double::sum);
            // }

            for (Map.Entry<String, Integer> entry : factorItem.sentimentCount.entrySet()) {
                acc.sentimentCount.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }

            // 个数相加
            // acc.recordNum += factorItem.recordNum;
        }
    }

    public void resetAccumulator(SentimentStatAvgAccumulator acc) {
        // acc.sentimentRatioMap =  new HashMap<>();
        acc.sentimentCount = new HashMap<>();
        // acc.recordNum = 0;
    }

    private String covertMap2Json(SentimentStatAvgAccumulator acc) {
        // 计算每个 factor 的平均值
        DecimalFormat df = new DecimalFormat("#.00"); // 定义格式，保留两位小数

        //
        Integer sentimentCountSum = 0;
        for (Map.Entry<String, Integer> entry : acc.sentimentCount.entrySet()) {
            sentimentCountSum += entry.getValue();
        }

        JSONArray resultArray = new JSONArray();
        for (String name : acc.sentimentCount.keySet()) {
            // 计算均值
            // Double average = acc.sentimentRatioMap.get(name) / (acc.recordNum == 0 ? 1 : acc.recordNum);
            Double average = (double) acc.sentimentCount.get(name) / (sentimentCountSum == 0 ? 1 : sentimentCountSum);
            average = Double.valueOf(df.format(average));
            JSONObject factorObject = new JSONObject();
            factorObject.put("name", name);
            factorObject.put("ratio", average);
            factorObject.put("count", acc.sentimentCount.get(name));
            resultArray.add(factorObject);
        }

        // TODO: 检查是否是100%

        return resultArray.toJSONString();
    }

}