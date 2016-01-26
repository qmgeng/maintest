package com.testjava.bean;

import java.util.Date;

/**
 * Created by qmgeng on 15/11/20.
 */
public class Student {
    private Long uuid;
    private String name;
    private int age;
    private Date ltime;

    public Long getUuid() {
        return uuid;
    }

    public void setUuid(Long uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getLtime() {
        return ltime;
    }

    public void setLtime(Date ltime) {
        this.ltime = ltime;
    }
}
