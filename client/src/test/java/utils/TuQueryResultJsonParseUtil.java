/**
 * Copyright (c) 2022 TuDB
 **/
package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import vo.TuClientQueryResultVO;
import vo.TuNodeVO;
import vo.TuRelationVO;

import java.util.*;

/**
 * tudb query result parse
 *
 * @author : johnny
 * @date : 2022/7/2
 **/
public class TuQueryResultJsonParseUtil {


    public static List<TuClientQueryResultVO> parseJsonList(String json) throws JsonProcessingException {
        List<TuClientQueryResultVO> resultList = new ArrayList<>();
        JSONArray list = JSONObject.parseArray(json);
        if (list == null || list.isEmpty()) {
            return resultList;
        }
        list.forEach(l -> {

            TuClientQueryResultVO tcqr = parseQueryResult((JSONArray) l);
            if (tcqr != null) {
                resultList.add(tcqr);
            }

        });
        return resultList;
    }

    private static TuClientQueryResultVO parseQueryResult(JSONArray arr) {
        TuClientQueryResultVO r = new TuClientQueryResultVO();
        Map<String, TuNodeVO> nodes = new HashMap<String, TuNodeVO>();
        Map<String, TuRelationVO> relations = new HashMap<String, TuRelationVO>();
        r.setNodes(nodes);
        r.setRelations(relations);

        arr.forEach(o -> {
            JSONObject obj = (JSONObject) o;
            String key = obj.getJSONArray("keys").getString(0);
            JSONObject fields = obj.getJSONArray("_fields").getJSONObject(0);
            if (fields.getInteger("start") != null) {
                TuRelationVO relation = parseRelation(fields);
                relations.put(key, relation);
            } else {
                TuNodeVO node = parseNode(fields);
                nodes.put(key, node);
            }
        });

        return r;

    }

    private static TuNodeVO parseNode(JSONObject inner) {
        TuNodeVO n = new TuNodeVO();
        n.setId(inner.getLong("identity"));
        JSONObject properties = (JSONObject) inner.get("properties");
        Map<String, String> pout = new HashMap<String, String>();
        Set<String> sets = properties.keySet();
        sets.forEach(key -> {
            JSONObject p = (JSONObject) properties.get(key);
            pout.put(key, (String) p.get("v"));
        });
        n.setProperties(pout);
        List<String> labels = (List<String>) inner.get("labels");
        n.setLabels(labels);
        return n;
    }

    private static TuRelationVO parseRelation(JSONObject inner) {
        TuRelationVO r = new TuRelationVO();
        r.setId(inner.getLong("identity"));
        r.setStartId(inner.getLong("start"));
        r.setEndId(inner.getLong("end"));
        r.setRelationType(inner.getString("rel"));
        JSONObject properties = (JSONObject) inner.get("properties");
        Map<String, String> pout = new HashMap<String, String>();
        Set<String> sets = properties.keySet();
        sets.forEach(key -> {
            JSONObject p = (JSONObject) properties.get(key);
            pout.put(key, (String) p.get("v"));
        });
        r.setProperties(properties);
        return r;
    }
}
