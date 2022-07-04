/**
 * Copyright (c) 2022 TuDB
 **/
package vo;

import java.util.List;
import java.util.Map;

/**
 * @author : johnny
 * @date : 2022/7/2
 **/
public class TuNodeVO {
    private long id;

    private List<String> labels;

    private Map<String,String> properties;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
