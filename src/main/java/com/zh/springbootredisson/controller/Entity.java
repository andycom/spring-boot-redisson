package com.zh.springbootredisson.controller;

/**
 * Created by qirong on 2019/8/13.
 */
public class Entity {

    private String name;

    private long age;

    public static Entity of(String name, long age) {
        Entity entity = new Entity();
        entity.age = age;
        entity.name = name;
        return entity;
    }

    public String getName() {
        return name;
    }

    public Entity setName(String name) {
        this.name = name;
        return this;
    }

    public long getAge() {
        return age;
    }

    public Entity setAge(long age) {
        this.age = age;
        return this;
    }
}
