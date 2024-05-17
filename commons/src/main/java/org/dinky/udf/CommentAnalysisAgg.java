package org.dinky.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.*;

/**
 * 字段comment_analysis示例
 * {"subjects": ["鸠兹", "古镇", "雪景", "芜湖"]}
 */
public class CommentAnalysisAgg extends AggregateFunction<String, CommentAnalysisAccumulator> {

    @Override
    public CommentAnalysisAccumulator createAccumulator() {
        return new CommentAnalysisAccumulator();
    }

    @Override
    public String getValue(CommentAnalysisAccumulator acc) {
        return covertMap2Json(acc);
    }

    public void accumulate(CommentAnalysisAccumulator acc, String anotherCommentAnalysis) {
        // 验证是否是默认值
        if (anotherCommentAnalysis.equals("")
                || anotherCommentAnalysis.equals("{}")
                || anotherCommentAnalysis.equals("{\"subjects\": []}")
                || anotherCommentAnalysis.equals("-1")) {
            return;
        }

        // 验证是否是合法json
        JSONObject jsonObject = null;
        try {
            // 解析 JSON 字符串为 JSONObject
            jsonObject = JSON.parseObject(anotherCommentAnalysis);
        } catch (Exception exception) {
            return;
        }

        // 提取 subjects 数组
        JSONArray jsonArray = jsonObject.getJSONArray("subjects");

        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            String subjectWord = jsonArray.getString(i);

            if (subjectWord == null) continue;
            // 累加占比
            acc.subjectCntMap.put(subjectWord, acc.subjectCntMap.getOrDefault(subjectWord, 0) + 1);
        }
    }

    /**
     * 非必须接口，用于撤销已经计算的中间结果的场景
     * @param acc
     * @param anotherCommentAnalysis
     */
    public void retract(CommentAnalysisAccumulator acc, String anotherCommentAnalysis) {
        JSONObject jsonObject = JSON.parseObject(anotherCommentAnalysis);

        // 提取 subjects 数组
        JSONArray jsonArray = jsonObject.getJSONArray("subjects");
        // 遍历 JSON 数组，计算每个 factor 的总和和计数
        for (int i = 0; i < jsonArray.size(); i++) {
            String subjectWord = jsonArray.getString(i);

            if (subjectWord == null) continue;
            // 累加占比
            acc.subjectCntMap.put(subjectWord, acc.subjectCntMap.getOrDefault(subjectWord, 0) - 1);
        }
    }

    public void merge(CommentAnalysisAccumulator acc, Iterable<CommentAnalysisAccumulator> iterableAcc) {
        for (CommentAnalysisAccumulator item : iterableAcc) {
            // HashMap合并到acc
            for (Map.Entry<String, Integer> entry : item.subjectCntMap.entrySet()) {
                acc.subjectCntMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }
    }

    public void resetAccumulator(CommentAnalysisAccumulator acc) {
        acc.subjectCntMap =  new HashMap<>();
    }

    private String covertMap2Json(CommentAnalysisAccumulator acc) {
        // 将 Map 的条目存入一个列表中
        List<Map.Entry<String, Integer>> list = new ArrayList<>(acc.subjectCntMap.entrySet());

        // 对列表按照 value 进行降序排序
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        // 提取排序后的 key
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : list) {
            keys.add(entry.getKey());
        }

        // 创建一个包含 keys 的 JSON 对象
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("subjects", keys);

        // 转换成 JSON 字符串
        String result = JSON.toJSONString(jsonObject);
        if (result.length() <= 1024) {
            return result;
        }

        // 探测最大长度后返回
        for (int right = keys.size() - 2; right > 0; right -= 10) {
            jsonObject.put("subjects", keys.subList(0, right));
            result = JSON.toJSONString(jsonObject);
            if (result.length() <= 1024) {
                return result;
            }
        }

        return "{}";
    }

}