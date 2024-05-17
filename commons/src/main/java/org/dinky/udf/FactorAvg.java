package org.dinky.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.AggregateFunction;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class FactorAvg extends AggregateFunction<String, FactorAvgAccumulator> {

    @Override
    public FactorAvgAccumulator createAccumulator() {
        return new FactorAvgAccumulator();
    }

    @Override
    public String getValue(FactorAvgAccumulator acc) {
        return covertMap2Json(acc);
    }

    public void accumulate(FactorAvgAccumulator acc, String anotherFactor) {
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

        Map<String, Double> factorRatioMap = new HashMap<>();

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String factor = obj.getString("factor");
            Double ratio = obj.getDouble("ratio");
            Double rate = obj.getDouble("rate");

            // 优先使用ratio的值
            if (ratio == null && rate != null) {
                ratio = rate;
            }
            // 如果ratio/rate都为null，应该是数据异常，此处先不考虑，便于即使抛出错误

            // 累加占比
            acc.factorRatioMap.put(factor, acc.factorRatioMap.getOrDefault(factor, 0.0) + ratio);
        }

        // 记录条数
        acc.factorCount += 1;
    }

    /**
     * 非必须接口，用于撤销已经计算的中间结果的场景
     * @param acc
     * @param anotherFactor
     */
    public void retract(FactorAvgAccumulator acc, String anotherFactor) {
        JSONArray jsonArray = JSON.parseArray(anotherFactor);
        Map<String, Double> factorRatioMap = new HashMap<>();

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String factor = obj.getString("factor");
            Double ratio = obj.getDouble("ratio");

            // 累加占比
            acc.factorRatioMap.put(factor, acc.factorRatioMap.getOrDefault(factor, 0.0) - ratio);
        }

        // 记录条数
        acc.factorCount -= 1;
    }

    public void merge(FactorAvgAccumulator acc, Iterable<FactorAvgAccumulator> iterableFactorFactorAvgAcc) {
        for (FactorAvgAccumulator factorItem : iterableFactorFactorAvgAcc) {
            // HashMap合并到acc
            for (Map.Entry<String, Double> entry : factorItem.factorRatioMap.entrySet()) {
                acc.factorRatioMap.merge(entry.getKey(), entry.getValue(), Double::sum);
            }

            // 个数相加
            acc.factorCount += factorItem.factorCount;
        }
    }

    public void resetAccumulator(FactorAvgAccumulator acc) {
        acc.factorRatioMap =  new HashMap<>();
        acc.factorCount = 0;
    }

    private String covertMap2Json(FactorAvgAccumulator acc) {
        // 计算每个 factor 的平均值
        DecimalFormat df = new DecimalFormat("#.00"); // 定义格式，保留两位小数

        JSONArray resultArray = new JSONArray();
        for (String factor : acc.factorRatioMap.keySet()) {
            // 计算均值
            Double average = acc.factorRatioMap.get(factor) / (acc.factorCount == 0 ? 1 : acc.factorCount);
            average = Double.valueOf(df.format(average));
            JSONObject factorObject = new JSONObject();
            factorObject.put("factor", factor);
            factorObject.put("ratio", average);
            resultArray.add(factorObject);
        }

        // TODO: 检查是否是100%

        return resultArray.toJSONString();
    }

}