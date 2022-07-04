/**
 * Copyright (c) 2022 TuDB
 **/
package vo;

import java.util.Map;

/**
 * @author : johnny
 * @date : 2022/7/2
 **/
public class TuRelationVO {

    private long id;

    private long startId;

    private long endId;

    private String relationType;

    private Map<String,Object> properties;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getStartId() {
        return startId;
    }

    public void setStartId(long startId) {
        this.startId = startId;
    }

    public long getEndId() {
        return endId;
    }

    public void setEndId(long endId) {
        this.endId = endId;
    }

    public String getRelationType() {
        return relationType;
    }

    public void setRelationType(String relationType) {
        this.relationType = relationType;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
