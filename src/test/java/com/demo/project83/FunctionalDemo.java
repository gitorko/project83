package com.demo.project83;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
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

public class FunctionalDemo {

    List<Customer> customerList = List.of(
            Customer.builder().name("Peter").city("london").age(32).build(),
            Customer.builder().name("Joe").city("paris").age(28).build(),
            Customer.builder().name("Marie").city("rome").age(31).build(),
            Customer.builder().name("Raj").city("delhi").age(33).build(),
            Customer.builder().name("Simon").city("london").age(26).build()
    );

    @Test
    public void oldVsNewTest() {

        // Now let's group all person by city in pre Java 8 world
        Map<String, List<Customer>> personByCity = new HashMap<>();
        for (Customer p : customerList) {
            if (!personByCity.containsKey(p.getCity())) {
                personByCity.put(p.getCity(), new ArrayList<>());
            }
            personByCity.get(p.getCity()).add(p);
        }
        System.out.println("Person grouped by cities : " + personByCity + "\n");

        // Let's see how we can group objects in Java 8
        personByCity = customerList.stream().collect(Collectors.groupingBy(Customer::getCity));
        System.out.println("Person grouped by cities in Java 8: " + personByCity + "\n");

        // Now let's group person by age
        Map<Integer, List<Customer>> personByAge = customerList.stream().collect(Collectors.groupingBy(Customer::getAge));
        System.out.println("Person grouped by age in Java 8: " + personByAge);
    }

    @Test
    public void predicateTest() {
        Predicate<String> strlen = (s) -> s.length() < 10;
        assertEquals(strlen.test("Apples"), true);
    }

    @Test
    public void consumerTest() {
        Consumer<String> consumerStr = (s) -> System.out.println(s.toLowerCase());
        consumerStr.accept("ABCDefghijklmnopQRSTuvWxyZ");

        Runnable r1 = () -> System.out.println("run 1");
        r1.run();

        Consumer<String> hello = name -> System.out.println("Hello, " + name);
        for (String name : Arrays.asList("Duke", "Mickey", "Minnie")) {
            hello.accept(name);
        }

        //example of a lambda made from an instance method
        Consumer<String> print = System.out::println;
        print.accept("Coming to you directly from a lambda...");

        List<Integer> numLst = Arrays.asList(1, 2, 3, 4, 5, 6);
        numLst.forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer t) {
                System.out.println("t = " + t);
            }
        });
    }

    @Test
    public void functionTest() {
        //Function example
        Function<Integer, String> converter = (num) -> Integer.toString(num);
        System.out.println("length of 26: " + converter.apply(26).length());

        //lambdas made using a constructor
        Function<String, BigInteger> newBigInt = BigInteger::new;
        System.out.println("expected value: 123456789, actual value: " +
                newBigInt.apply("123456789"));
    }

    @Test
    public void supplierTest() {
        //Supplier example
        Supplier<String> s = () -> "Java is fun";
        System.out.println(s.get());
    }

    @Test
    public void binaryOperatorTest() {
        //Binary Operator example
        BinaryOperator<Integer> add = (a, b) -> a + b;
        System.out.println("add 10 + 25: " + add.apply(10, 25));
    }

    @Test
    public void unaryOperatorTest() {
        //Unary Operator example
        UnaryOperator<String> str = (msg) -> msg.toUpperCase();
        System.out.println(str.apply("This is my message in upper case"));

        //these two are the same using the static method concat
        UnaryOperator<String> greeting = x -> "Hello, ".concat(x);
        System.out.println(greeting.apply("World"));

        UnaryOperator<String> makeGreeting = "Hello, "::concat;
        System.out.println(makeGreeting.apply("Peggy"));
    }

    @Test
    public void biFunctionTest() {
        BiFunction<String, String, String> concat = (a, b) -> a + b;
        String sentence = concat.apply("Today is ", "a great day");
        System.out.println(sentence);
    }

    @Test
    public void functionalInterfaceTest() {
        GreetingFunction greeting = message ->
                System.out.println("Java Programming " + message);
        greeting.sayMessage("Rocks with lambda expressions");
    }

    @Test
    public void intFunctionTest() {
        IntFunction<String> intToString = num -> Integer.toString(num);
        System.out.println("expected value 3, actual value: " +
                intToString.apply(123).length());

        //static method reference using ::
        IntFunction<String> intToString2 = Integer::toString;
        System.out.println("expected value 4, actual value:  " +
                intToString2.apply(4567).length());
    }

    @Test
    public void collectTest() {
        //Collect customers who are below 30.
        List<Customer> result = customerList.stream()
                .filter(e -> e.getAge() < 30)
                .collect(toList());
        assertEquals(result.size(), 2);

        //get all employee names in List<String>
        ArrayList<String> result2 = customerList.stream()
                .map(e -> e.getName())
                .collect(Collectors.toCollection(ArrayList::new));
        assertEquals(result2.size(), 5);

        //one to one
        List<Integer> nums = List.of(1, 2, 3);
        List<Integer> result3 = nums.stream()
                .map(e -> e * 2)
                .collect(toList());
        System.out.println(result3);

        //name and age to map.
        customerList.stream()
                .filter(e -> e.getAge() > 30)
                .collect(Collectors.toMap(Customer::getName, Customer::getAge))
                .forEach((k, v) -> System.out.println(k + ":" + v));

        //to list.
        customerList.stream()
                .filter(e -> e.getAge() > 30)
                .collect(toList())
                .forEach(System.out::println);

        String result4 = customerList.stream()
                .filter(e -> e.getAge() > 30)
                .map(e -> e.getName())
                .map(e -> e.toUpperCase())
                .collect(Collectors.joining(", "));
        System.out.println(result4);

        customerList.stream()
                .filter(e -> e.getAge() > 30)
                .map(e -> e.getName())
                .map(String::toUpperCase)
                .collect(toList())
                .forEach(System.out::println);
    }

    @Test
    void collectToMapTest() {
        Map<String, Integer> map = new HashMap<>();
        map.put("Niraj", 6);
        map.put("Rahul", 43);
        map.put("Ram", 44);
        map.put("Sham", 33);
        map.put("Pratik", 5);
        map.put("Ashok", 5);

        //Sort map by Value Ascending order
        Map<String, Integer> sortedMapByValueAscending
                =  map.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        //Sort map by Value Descending orde
        Map<String, Integer> sortedMapByValueDescending
                = map.entrySet().stream()
                .sorted(Map.Entry.<String,Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1 ,LinkedHashMap::new));

        //Sort map by Key Ascending order
        Map<String, Integer> sortedMapByKeyAscending
                = map.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,LinkedHashMap::new));

        //Sort map by Key Descending order
        Map<String, Integer> sortedMapByKeyDescending
                = map.entrySet()
                .stream().sorted(Map.Entry.<String,Integer>comparingByKey().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1 ,LinkedHashMap::new));

    }

    @Test
    public void collectSumTest() {
        //Sum all ages.
        int total = customerList.stream().collect(Collectors.summingInt(Customer::getAge));
        assertEquals(total, 150);
    }

    @Test
    public void sortTest() {
        List<String> names = Arrays.asList("Paul", "Jane", "Michaela", "Sam");
        Collections.sort(names, (a, b) -> b.compareTo(a));
        names.forEach(System.out::println);
    }

    @Test
    public void filterTest() {
        Arrays.asList("red", "green", "blue")
                .stream()
                .sorted()
                .findFirst()
                .ifPresent(System.out::println);

        //example of Stream.of with a filter
        Stream.of("apple", "pear", "banana", "cherry", "apricot")
                .filter(fruit -> {
                    //  System.out.println("filter: " + fruit);
                    return fruit.startsWith("a"); //predicate
                })
                //if the foreach is removed, nothing will print,
                //the foreach makes it a terminal event
                .forEach(fruit -> System.out.println("Starts with A: " + fruit));

        //using a stream and map operation to create a list of words in caps
        List<String> collected = Stream.of("Java", " Rocks")
                .map(string -> string.toUpperCase())
                .collect(toList());
        System.out.println(collected.toString());
    }

    @Test
    public void mapToIntTest() {
        //primitive streams
        IntStream.range(1, 4)
                .forEach(System.out::println);

        //find the average of the numbers squared
        Arrays.stream(new int[]{1, 2, 3, 4})
                .map(n -> n * n)
                .average()
                .ifPresent(System.out::println);

        //map doubles to ints
        Stream.of(1.5, 2.3, 3.7)
                .mapToInt(Double::intValue)
                .forEach(System.out::println);

        OptionalInt max = customerList.stream()
                .mapToInt(Customer::getAge)
                .max();
        System.out.println(max.getAsInt());

        int sum = customerList.stream()
                .mapToInt(Customer::getAge)
                .sum();
        System.out.println(sum);
    }

    @Test
    public void doubleSortTest() {
        //Sort customer by name and then by age.
        customerList.stream()
                .sorted(
                        Comparator.comparing(Customer::getName)
                                .thenComparing(Customer::getAge)
                )
                .forEach(System.out::println);
    }

    @Test
    public void flatMapTest() {
        //Get chars of all customer names.
        List<String> collect = customerList.stream()
                .map(Customer::getName)
                .flatMap(name -> Stream.of(name.split("")))
                .collect(toList());
        System.out.println(collect);

        //one to many
        List<Integer> nums = List.of(1, 2, 3);
        List<Integer> collect2 = nums.stream()
                .flatMap(e -> List.of(e, e + 1).stream())
                .collect(toList());
        System.out.println(collect2);
    }

    @Test
    public void groupByFilterTest() {
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
        func1 = FunctionalDemo::concat1;
        System.out.println(func1.apply("Hello", "World2"));

        //instance method.
        func1 = new FunctionalDemo()::concat2;
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
