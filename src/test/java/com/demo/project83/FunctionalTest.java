package com.demo.project83;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FunctionalTest {

    List<Customer> customerList = List.of(
            Customer.builder().name("Peter Parker").city("london").age(32).build(),
            Customer.builder().name("Joe").city("paris").age(28).build(),
            Customer.builder().name("Marie").city("rome").age(31).build(),
            Customer.builder().name("Peter").city("rome").age(30).build(),
            Customer.builder().name("Raj").city("delhi").age(33).build(),
            Customer.builder().name("Simon").city("london").age(26).build()
    );

    /**
     * ********************************************************************
     *  Difference between imperative vs functional style
     * ********************************************************************
     */
    @Test
    public void imperativeVsFunctional() {

        // Group all person by city in pre Java 8 world
        Map<String, List<Customer>> personByCity1 = new HashMap<>();
        for (Customer p : customerList) {
            if (!personByCity1.containsKey(p.getCity())) {
                personByCity1.put(p.getCity(), new ArrayList<>());
            }
            personByCity1.get(p.getCity()).add(p);
        }
        System.out.println("Person grouped by cities : " + personByCity1);
        assertEquals(2, personByCity1.get("rome").size());
        System.out.println("---------------------------------------------------");

        // Group objects in Java 8
        Map<String, List<Customer>> personByCity2 = customerList.stream()
                .collect(Collectors.groupingBy(Customer::getCity));
        System.out.println("Person grouped by cities in Java 8: " + personByCity2);
        assertEquals(2, personByCity2.get("rome").size());
        System.out.println("---------------------------------------------------");

        // Now let's group person by age
        Map<Integer, List<Customer>> personByAge = customerList.stream().collect(Collectors.groupingBy(Customer::getAge));
        System.out.println("Person grouped by age in Java 8: " + personByAge);
        assertEquals(1, personByAge.get(32).size());
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Predicate <T> - takes T returns boolean
     * ********************************************************************
     */
    @Test
    public void predicateTest() {
        Predicate<String> strlen = (s) -> s.length() < 10;
        assertEquals(strlen.test("Apples"), true);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Runnable - takes nothing returns nothing
     * ********************************************************************
     */
    @Test
    public void runnableTest() {
        Runnable emptyConsumer = () -> System.out.println("run 1");
        emptyConsumer.run();
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Consumer <T> - takes T returns nothing
     * ********************************************************************
     */
    @Test
    public void consumerTest() {
        Consumer<String> consumerStr = (s) -> System.out.println(s.toUpperCase());
        consumerStr.accept("peter parker");
        System.out.println("---------------------------------------------------");

        Consumer<String> hello = name -> System.out.println("Hello, " + name);
        customerList.forEach(c -> hello.accept(c.getName()));
        System.out.println("---------------------------------------------------");

        //example of a lambda made from an instance method
        Consumer<String> print = System.out::println;
        print.accept("Sent directly from a lambda...");
        System.out.println("---------------------------------------------------");

        //As anonymous class, dont use this, provided for explanation only.
        customerList.forEach(new Consumer<Customer>() {
            @Override
            public void accept(Customer customer) {
                System.out.println("Hello " + customer.getName());
            }
        });
        System.out.println("---------------------------------------------------");

    }

    /**
     * ********************************************************************
     *  Function <T,R> - takes T returns R
     * ********************************************************************
     */
    @Test
    public void functionTest() {
        //Function example
        Function<Integer, String> convertNumToString = (num) -> Integer.toString(num);
        System.out.println("String value is : " + convertNumToString.apply(26));
        System.out.println("---------------------------------------------------");

        //lambdas made using a constructor
        Function<String, BigInteger> newBigInt = BigInteger::new;
        System.out.println("Number " + newBigInt.apply("123456789"));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Supplier <T> - takes nothing returns T
     * ********************************************************************
     */
    @Test
    public void supplierTest() {
        Supplier<String> s = () -> "Message from supplier";
        System.out.println(s.get());
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  BinaryOperator <T> - takes T,T returns T
     * ********************************************************************
     */
    @Test
    public void binaryOperatorTest() {
        BinaryOperator<Integer> add = (a, b) -> a + b;
        System.out.println("add 10 + 25: " + add.apply(10, 25));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  UnaryOperator <T> - takes T returns T
     * ********************************************************************
     */
    @Test
    public void unaryOperatorTest() {
        UnaryOperator<String> str = (msg) -> msg.toUpperCase();
        System.out.println(str.apply("hello, Joe"));
        System.out.println("---------------------------------------------------");

        //same example but using the static method concat
        UnaryOperator<String> greeting = x -> "Hello, ".concat(x);
        System.out.println(greeting.apply("Raj"));
        System.out.println("---------------------------------------------------");

        UnaryOperator<String> makeGreeting = "Hello, "::concat;
        System.out.println(makeGreeting.apply("Peggy"));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  BiFunction <T,R,S> - takes T,R returns S
     * ********************************************************************
     */
    @Test
    public void biFunctionTest() {
        BiFunction<Integer, Boolean, String> concat = (a, b) -> a.toString() + b.toString();
        System.out.println(concat.apply(23, true));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Custom Functional Interface
     * ********************************************************************
     */
    @Test
    public void functionalInterfaceTest() {
        GreetingFunction greeting = message ->
                System.out.println("Java Programming " + message);
        greeting.sayMessage("is awesome");
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  IntFunction<T> - takes integer returns T
     * ********************************************************************
     */
    @Test
    public void intFunctionTest() {
        IntFunction<String> intToString = num -> Integer.toString(num);
        System.out.println("String value of number: " + intToString.apply(123));
        System.out.println("---------------------------------------------------");

        //static method reference
        IntFunction<String> intToString2 = Integer::toString;
        System.out.println("String value of number: " + intToString2.apply(4567));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  Higher order function - pass functions as arguments
     * ********************************************************************
     */
    @Test
    public void higherOrderTest() {
        //Function takes Integer,Predicate and returns Predicate
        //Function<T,R>
        Function<Integer, Predicate<String>> checkLength = (minLen) -> {
            //predicate returned
            return (str) -> str.length() > minLen;
        };
        List<String> collect = customerList.stream()
                .map(Customer::getName)
                .filter(checkLength.apply(4))
                .collect(toList());
        collect.forEach(System.out::println);
        assertEquals(4, collect.size());
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  collect - toList, joining, toCollection
     * ********************************************************************
     */
    @Test
    public void collectTest() {
        //Collect customers who are below 30.
        List<Customer> result = customerList.stream()
                .filter(e -> e.getAge() < 30)
                .collect(toList());
        assertEquals(2, result.size());
        System.out.println("---------------------------------------------------");

        //get all employee names in List<String>
        //Using toCollection you can specify the type
        ArrayList<String> result2 = customerList.stream()
                .map(e -> e.getName())
                .collect(Collectors.toCollection(ArrayList::new));
        assertEquals(6, result2.size());
        System.out.println("---------------------------------------------------");

        //Collect and join to single string separated by coma.
        String customerString = customerList.stream()
                .filter(e -> e.getAge() > 30)
                .map(e -> e.getName())
                .collect(Collectors.joining(", "));
        System.out.println(customerString);
        assertEquals("Peter Parker, Marie, Raj", customerString);
        System.out.println("---------------------------------------------------");

    }

    /**
     * ********************************************************************
     *  collect - toMap
     * ********************************************************************
     */
    @Test
    void collectToMapTest() {

        //Collect a map with name as key and age as value.
        customerList.stream()
                .filter(e -> e.getAge() > 30)
                .collect(Collectors.toMap(Customer::getName, Customer::getAge))
                .forEach((k, v) -> System.out.println(k + ":" + v));
        System.out.println("---------------------------------------------------");

        //Collect a map by name + city as key customer as value
        customerList.stream()
                .collect(Collectors.toMap(c -> c.getName() + "-" + c.getCity(), c -> c))
                .forEach((k, v) -> System.out.println(k + ":" + v));
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  collect - sort a Map by key or value
     * ********************************************************************
     */
    @Test
    public void sortMapTest() {
        Map<String, Integer> map = new HashMap<>();
        map.put("Niraj", 6);
        map.put("Rahul", 43);
        map.put("Ram", 44);
        map.put("Sham", 33);
        map.put("Pratik", 5);
        map.put("Ashok", 5);

        //Sort map by Value Ascending order
        Map<String, Integer> sortedMapByValueAscending =  map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        System.out.println(sortedMapByValueAscending);
        System.out.println("---------------------------------------------------");

        //Sort map by Value Descending order
        Map<String, Integer> sortedMapByValueDescending = map.entrySet()
                .stream()
                .sorted(Map.Entry.<String,Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1 ,LinkedHashMap::new));
        System.out.println(sortedMapByValueDescending);
        System.out.println("---------------------------------------------------");

        //Sort map by Key Ascending order
        Map<String, Integer> sortedMapByKeyAscending
                = map.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,LinkedHashMap::new));
        System.out.println(sortedMapByKeyAscending);
        System.out.println("---------------------------------------------------");

        //Sort map by Key Descending order
        Map<String, Integer> sortedMapByKeyDescending
                = map.entrySet()
                .stream().sorted(Map.Entry.<String,Integer>comparingByKey().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1 ,LinkedHashMap::new));
        System.out.println(sortedMapByKeyDescending);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  collect - summingInt, sum
     * ********************************************************************
     */
    @Test
    public void collectSumTest() {
        //Sum all ages.
        int total = customerList.stream()
                .collect(Collectors.summingInt(Customer::getAge));
        assertEquals(total, 180);
        System.out.println("---------------------------------------------------");

        int total2 = customerList.stream()
                .mapToInt(Customer::getAge)
                .sum();
        assertEquals(total2, 180);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  sorted
     * ********************************************************************
     */
    @Test
    public void sortedTest() {

        List<String> sortResult = customerList.stream()
                .map(c -> c.getName())
                .sorted((a, b) -> b.compareTo(a))
                .collect(toList());
        sortResult.forEach(System.out::println);

        //Avoid using the below as it modifies the orignial list.
        //Collections.sort(customerList, (a, b) -> b.getName().compareTo(a.getName()));

        List<String> expectedResult = List.of("Simon","Raj","Peter Parker","Peter","Marie","Joe");
        assertEquals(expectedResult, sortResult);
        System.out.println("---------------------------------------------------");

    }

    /**
     * ********************************************************************
     *  filter
     * ********************************************************************
     */
    @Test
    public void filterTest() {
        customerList.stream()
                .filter(customer -> {
                    return customer.getName().startsWith("P"); //predicate
                })
                .forEach(System.out::println);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  findFirst, ifPresent
     * ********************************************************************
     */
    @Test
    public void findFirstTest() {
        customerList
                .stream()
                .filter(customer -> customer.getName().startsWith("P"))
                .findFirst()
                .ifPresent(System.out::println);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  mapToInt, max, average, IntStream
     * ********************************************************************
     */
    @Test
    public void mapToIntTest() {
        int sum = customerList.stream()
                .mapToInt(Customer::getAge)
                .sum();
        System.out.println(sum);
        System.out.println("---------------------------------------------------");

        //primitive streams
        IntStream.range(1, 4)
                .forEach(System.out::println);
        System.out.println("---------------------------------------------------");

        //find the average of the numbers squared
        Arrays.stream(new int[]{1, 2, 3, 4})
                .map(n -> n * n)
                .average()
                .ifPresent(System.out::println);
        System.out.println("---------------------------------------------------");

        //map doubles to ints
        Stream.of(1.5, 2.3, 3.7)
                .mapToInt(Double::intValue)
                .forEach(System.out::println);
        System.out.println("---------------------------------------------------");

        //max of age
        OptionalInt max = customerList.stream()
                .mapToInt(Customer::getAge)
                .max();
        System.out.println(max.getAsInt());
        System.out.println("---------------------------------------------------");

    }

    /**
     * ********************************************************************
     *  thenComparing - double sort, sort on name, then sort on age
     * ********************************************************************
     */
    @Test
    public void doubleSortTest() {
        //Sort customer by name and then by age.
        customerList.stream()
                .sorted(
                        Comparator.comparing(Customer::getName)
                                .thenComparing(Customer::getAge)
                )
                .forEach(System.out::println);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  flatMap
     * ********************************************************************
     */
    @Test
    public void flatMapTest() {
        //Get chars of all customer names.
        Set<String> collect = customerList.stream()
                .map(Customer::getName)
                .flatMap(name -> Stream.of(name.split("")))
                .collect(toSet());
        System.out.println(collect);
        System.out.println("---------------------------------------------------");

        //one to many
        List<Integer> nums = List.of(1, 2, 3);
        List<Integer> collect2 = nums.stream()
                .flatMap(e -> List.of(e, e + 1).stream())
                .collect(toList());
        System.out.println(collect2);
        System.out.println("---------------------------------------------------");
    }

    /**
     * ********************************************************************
     *  collect - groupBy
     * ********************************************************************
     */
    @Test
    public void groupByFilterTest() {

        //group by name and get list of customers with same name.
        customerList.stream()
                .collect(groupingBy(Customer::getName))
                .forEach((k, v) -> System.out.println(k + ":" + v));

        //group by name and get list of ages if customer with same name.
        customerList.stream()
                .collect(groupingBy(Customer::getName, mapping(Customer::getAge, toList())))
                .forEach((k, v) -> System.out.println(k + ":" + v));

        //Group by age, employees who name is greater than 4 chars.
        Map<Integer, List<String>> result = customerList.stream()
                .collect(
                        Collectors.groupingBy(Customer::getAge,
                                Collectors.mapping(
                                        Customer::getName,
                                        Collectors.filtering(name -> name.length() > 4, toList())
                                ))
                );
        System.out.println(result);
    }

    @Test
    public void groupByTest() {
        //group by age all customers.
        Map<Integer, List<String>> result = customerList.stream()
                .collect(
                        Collectors.groupingBy(Customer::getAge,
                                Collectors.mapping(Customer::getName, toList()))
                );
        System.out.println(result);

        //count emp with same name.
        Map<String, Long> result2 = customerList.stream()
                .collect(Collectors.groupingBy(Customer::getName, Collectors.counting()));
        System.out.println(result2);

        //Group all same name but put age to list.
        Map<String, List<Integer>> result3 = customerList.stream()
                .collect(Collectors
                        .groupingBy(Customer::getName, Collectors.mapping(Customer::getAge, toList())));
        System.out.println(result3);

        //group all by same name
        Map<String, List<Customer>> result4 = customerList.stream()
                .collect(Collectors.groupingBy(p -> p.getName()));
        System.out.println(result4);
    }

    @Test
    public void maxByTest() {
        //emp with max age and print name instead of emp.
        String result = customerList.stream()
                .collect(Collectors.collectingAndThen(
                        Collectors.maxBy(Comparator.comparing(Customer::getAge)),
                        e -> e.map(Customer::getName).orElse("")
                        )
                );
        System.out.println(result);

        //emp with max age
        Optional<Customer> maxEmp = customerList.stream()
                .collect(Collectors.maxBy(Comparator.comparing(Customer::getAge)));
        System.out.println(maxEmp.get());
    }

    @Test
    public void collectingAndThenTest() {
        //convert long to int.
        Map<String, Integer> result = customerList.stream()
                .collect(Collectors.groupingBy(Customer::getName,
                        Collectors.collectingAndThen(Collectors.counting(),
                                Long::intValue
                        )));
        System.out.println(result);
    }

    @Test
    public void partitioningByTest() {
        //2 list of even odd employees
        Map<Boolean, List<Customer>> result = customerList.stream()
                .collect(Collectors.partitioningBy(p -> p.getAge() % 2 == 0));
        System.out.println(result);
    }

    @Test
    public void reduceTest() {
        List<Integer> numLst = Arrays.asList(1, 2, 3, 4, 5, 6);

        //Sum of integer array. (both are param)
        Integer reduce = numLst.stream().reduce(0, (total, val) -> Integer.sum(total, val));
        System.out.println("reduce = " + reduce);

        reduce = numLst.stream().reduce(0, Integer::sum);
        System.out.println("reduce = " + reduce);

        //Concat of string. (one is target, one is param)
        String concat = numLst.stream().map(String::valueOf).reduce("", (carry, str) -> carry.concat(str));
        System.out.println("concat = " + concat);

        concat = numLst.stream().map(String::valueOf).reduce("", String::concat);
        System.out.println("concat = " + concat);

        Integer sum = numLst.stream().filter(e -> e % 2 == 0).map(e -> e * 2).reduce(0, Integer::sum);
        System.out.println("sum = " + sum);

        Integer sum2 = numLst.stream().filter(e -> e % 2 == 0).mapToInt(e -> e * 2).sum();
        System.out.println("sum2 = " + sum2);

        customerList.stream()
                .filter(e -> e.getAge() > 30)
                .map(e -> e.getName())
                .map(String::toUpperCase)
                .reduce(new ArrayList<String>(), (names, name) -> {
                            names.add(name);
                            return names;
                        },
                        (names1, names2) -> {
                            names1.addAll(names2);
                            return names1;
                        }
                ).forEach(System.out::println);
    }

    @Test
    public void ifPresentTest() {
        String input = "key:a,key:b,key:c,key:d";
        Optional.ofNullable(input)
                .ifPresent(in -> Arrays.stream(in.split( "," ))
                        .map(String::toLowerCase)
                        .peek(System.out::println)
                        .filter(not(match -> (match.startsWith("key"))))
                        .findAny()
                        .ifPresent(match -> new RuntimeException("Pattern not valid!")));
        System.out.println();
        String input2 = "key:a,key:b,:c,key:d";
        Assertions.assertThrows(RuntimeException.class, () -> {
            Optional.ofNullable(input2)
                    .ifPresent(in -> Arrays.stream(in.split( "," ))
                            .map(String::toLowerCase)
                            .peek(System.out::println)
                            .filter(not(match -> (match.startsWith("key"))))
                            .findAny()
                            .ifPresent(match -> {
                                System.out.println("Here!");
                                throw new RuntimeException("Pattern not valid!");
                            }));
        });
    }

    @Test
    public void higherOrderFunctionTest() {
        //inline lambda
        BiFunction<String, String, String> func1 = (s, t) -> {
            return s + t;
        };
        System.out.println(func1.apply("Hello", "World1"));

        //static method
        func1 = FunctionalTest::concat1;
        System.out.println(func1.apply("Hello", "World2"));

        //instance method.
        func1 = new FunctionalTest()::concat2;
        System.out.println(func1.apply("Hello", "World3"));

        //passing function as param
        System.out.println(
                concatAndTransform("Hello", "World4", (s) -> {
                    return s.toUpperCase();
                }));

        //Higher order function
        Supplier<String> formOps = concatAndTransformReturnFunction("Hello", "World5", (s) -> {
            return s.toUpperCase();
        });
        System.out.println(formOps.get());
    }

    private static String concat1(String a, String b) {
        return a + b;
    }

    private String concat2(String a, String b) {
        return a + b;
    }

    private static String concatAndTransform(String a, String b, Function<String, String> stringTransform) {
        if (stringTransform != null) {
            a = stringTransform.apply(a);
            b = stringTransform.apply(b);
        }
        return a + b;
    }

    private static Supplier<String> concatAndTransformReturnFunction(String a, String b, Function<String, String> stringTransform) {
        return () -> {
            String aa = a;
            String bb = b;
            //Because you can modify things in lambda.
            if (stringTransform != null) {
                aa = stringTransform.apply(aa);
                bb = stringTransform.apply(bb);
            }
            return aa + bb;
        };
    }

}

@Builder
@AllArgsConstructor
@Data
class Customer {
    public String name;
    public String city;
    public Integer age;
}

@FunctionalInterface
interface GreetingFunction {
    void sayMessage(String message);
}
