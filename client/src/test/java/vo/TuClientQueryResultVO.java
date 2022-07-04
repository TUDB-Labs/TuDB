/**
 * Copyright (c) 2022 TuDB
 **/
package vo;

import java.util.Map;

/**
 * @author : johnny
 * @date : 2022/7/2
 **/
public class TuClientQueryResultVO {
    private Map<String, TuNodeVO> nodes;
    private Map<String, TuRelationVO> relations;

    public Map<String, TuNodeVO> getNodes() {
        return nodes;
    }

    public void setNodes(Map<String, TuNodeVO> nodes) {
        this.nodes = nodes;
    }

    public Map<String, TuRelationVO> getRelations() {
        return relations;
    }

    public void setRelations(Map<String, TuRelationVO> relations) {
        this.relations = relations;
    }
}
