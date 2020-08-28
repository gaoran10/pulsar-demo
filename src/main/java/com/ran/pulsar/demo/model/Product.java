package com.ran.pulsar.demo.model;

import lombok.Data;

@Data
public class Product {

    private String name;
    private Double price;
    private Long updateTime;
    private String updateTimeStr;

}
