package com.demo.project83.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Employee {

    private String title;
    private int level;
    private List<Employee> directReports;

    public Employee(String title) {
        this.title = title;
        this.directReports = new ArrayList<>();
    }

    public void addDirectReports(Employee... employees) {
        Stream.of(employees).forEach(e -> e.setLevel(this.getLevel() + 1));
        directReports.addAll(List.of(employees));
    }

    @Override
    public String toString() {
        return "\t".repeat(this.level) + this.title;
    }
}
