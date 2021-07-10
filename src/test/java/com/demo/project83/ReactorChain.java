package com.demo.project83;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

@Slf4j
public class ReactorChain {

    @Test
    public void test() {
        CompanyVO request = new CompanyVO();
        request.setName("Twitter");
        Mono.just(request)
                .map(ReactorChain::convertToEntity)
                .flatMap(ReactorChain::addCompanyOwner)
                .flatMap(ReactorChain::appendOrgIdToDepartment)
                .flatMap(ReactorChain::save)
                .subscribe(System.out::println);
    }

    private static Mono<String> getOwnerName() {
        return Mono.just("Jack");
    }

    private static Mono<String> getOrgId() {
        return Mono.just("org1: ");
    }

    public static Company convertToEntity(CompanyVO companyVO) {
        Company company = new Company();
        company.setName(companyVO.getName().toUpperCase());
        List<Department> departments = new ArrayList<>();
        departments.add(Department.builder().name("department 1").build());
        company.setDepartments(departments);
        return company;
    }

    public static Mono<Company> save(Company company) {
        log.info("Saved to db!");
        return Mono.just(company);
    }

    public static Mono<Company> appendOrgIdToDepartment(Company company) {
        return getOrgId().map(e -> {
            company.getDepartments().forEach(d -> d.setName(e + " " + d.getName()));
            return company;
        });
    }

    public static Mono<Company> addCompanyOwner(Company company) {
        return getOwnerName().map(e -> {
            company.setOwner(e);
            return company;
        });
    }
}

@Data
class CompanyVO {
    String name;
}

@Data
class Company {
    String name;
    List<Department> departments;
    String owner;
}

@Data
@Builder
class Department {
    String name;
}
