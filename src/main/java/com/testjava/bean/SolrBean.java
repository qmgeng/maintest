package com.testjava.bean;

import org.apache.solr.client.solrj.beans.Field;

/**
 * Created by qmgeng on 15/12/1.
 */
public class SolrBean {
    @Field("id")
    private String id;
    @Field("type")
    private String type;
    @Field("source")
    private String source;
    @Field("editor1")
    private String editor1;
    @Field("editor2")
    private String editor2;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getEditor1() {
        return editor1;
    }

    public void setEditor1(String editor1) {
        this.editor1 = editor1;
    }

    public String getEditor2() {
        return editor2;
    }

    public void setEditor2(String editor2) {
        this.editor2 = editor2;
    }
}
