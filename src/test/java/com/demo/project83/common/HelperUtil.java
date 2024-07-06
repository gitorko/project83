package com.demo.project83.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class HelperUtil {

    public static String getName() {
        return "jack";
    }

    public static Mono<Customer> capitalizeCustomerName(Customer customer) {
        return Mono.just(customer)
                .map(c -> {
                    c.setName(c.getName().toUpperCase());
                    return c;
                })
                .delayElement(Duration.ofMillis(new Random().nextInt(1000)));
    }

    public static Flux<Customer> capitalizeCustomerName(List<Customer> customers) {
        return Flux.fromIterable(customers)
                .map(c -> {
                    c.setName(c.getName().toUpperCase());
                    return c;
                })
                .delayElements(Duration.ofMillis(new Random().nextInt(1000)));
    }

    public static Mono<String> capitalizeReactive(String user) {
        return Mono.just(user.toUpperCase())
                .delayElement(Duration.ofMillis(new Random().nextInt(1000)));
    }

    public static String capitalize(String user) {
        return user.toUpperCase();
    }

    public static Customer getCustomer() {
        return Customer.builder().name("jack").age(32).city("new york").build();
    }

    public static List<Customer> getCustomers() {
        List<Customer> customers = new ArrayList<>();
        customers.add(Customer.builder().name("jack").age(32).city("new york").build());
        customers.add(Customer.builder().name("raj").age(40).city("bangalore").build());
        customers.add(Customer.builder().name("peter").city("london").age(32).build());
        customers.add(Customer.builder().name("joe").city("paris").age(28).build());
        customers.add(Customer.builder().name("marie").city("rome").age(31).build());
        return customers;
    }

    public static Customer changeCity(Customer customer, String newCity) {
        customer.setCity(newCity);
        return customer;
    }

    public static String checkName(String name) throws CustomException {
        if (name.equals("Jill")) {
            throw new CustomException();
        }
        return name.toUpperCase();
    }

    @SneakyThrows
    public static String capitalizeString(String element) {
        log.info("Capitalizing: {}", element);
        longOps();
        return element.toUpperCase();
    }

    @SneakyThrows
    public static String capitalizeStringLatch(String element, CountDownLatch latch) {
        log.info("Capitalizing: {}", element);
        longOps();
        latch.countDown();
        return element.toUpperCase();
    }

    public static Flux<String> deleteFromDb() {
        return Flux.just("Deleted from db").log();
    }

    public static Flux<String> saveToDb() {
        return Flux.just("Saved to db").log();
    }

    public static Mono<Void> sendMail() {
        return Mono.empty();
    }

    public static Company appendSuffix(Company company, String nameSuffix) {
        company.setName(company.name + " " + nameSuffix);
        return company;
    }

    public static Mono<String> getOwnerName() {
        return Mono.just("Jack");
    }

    public static Mono<String> getOrgId() {
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

    public static Mono<String> getNameSuffix() {
        return Mono.just(".Inc");
    }

    public static Mono<Company> addCompanyOwner(Company company) {
        return getOwnerName().map(e -> {
            company.setOwner(e);
            return company;
        });
    }

    public static void longOps() {
        long j = 0;
        for (int i = 0; i < 9999999; i++) {
            j++;
        }
    }

    @SneakyThrows
    public static void sleep(long seconds) {
        TimeUnit.SECONDS.sleep(seconds);
    }

    public static final class CustomException extends Exception {
        public static final long serialVersionUID = 0L;
    }
}



