package com.demo.project83.common;

import java.util.List;

import lombok.Data;

@Data
public class Company {
    String name;
    List<Department> departments;
    String owner;
}