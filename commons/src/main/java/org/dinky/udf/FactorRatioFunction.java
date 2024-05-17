package org.dinky.udf;

import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class FactorRatioFunction extends ScalarFunction {

    /**
     * 富足
     * @param jsonString [{"factor": "景观质量", "ratio": 22.9},
     *                   {"factor": "经营管理", "ratio": 27.13},
     *                   {"factor": "接待能力", "ratio": 19.68},
     *                   {"factor": "体验", "ratio": 30.29}]
     * @return
     */
    public String eval(String jsonString) {
        JSONArray jsonArray = JSON.parseArray(jsonString);
        Map<String, Double> factorSum = new HashMap<>();
        Map<String, Integer> factorCount = new HashMap<>();

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            String factor = obj.getString("factor");
            Double ratio = obj.getDouble("ratio");
            factorSum.put(factor, factorSum.getOrDefault(factor, 0.0) + ratio);
            factorCount.put(factor, factorCount.getOrDefault(factor, 0) + 1);
        }

        // 计算每个 factor 的平均值
        JSONArray resultArray = new JSONArray();
        for (String factor : factorSum.keySet()) {
            Double average = factorSum.get(factor) / factorCount.get(factor);
            JSONObject factorObject = new JSONObject();
            factorObject.put("factor", factor);
            factorObject.put("average_ratio", average);
            resultArray.add(factorObject);
        }

        return resultArray.toJSONString();
    }

}
