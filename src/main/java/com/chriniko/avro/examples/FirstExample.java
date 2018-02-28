package com.chriniko.avro.examples;


import com.chriniko.avro.domain.Employee;

public class FirstExample implements Example {

    @Override
    public void run() {


        Employee employee = Employee.newBuilder()
                .setId(1)
                .setEmailAddress("emp1@comp.com")
                .setFirstName("first1")
                .setMiddleName("mid1")
                .setLastName("last1")
                .setUsername("user1")
                .build();


        System.out.println("employee == " + employee);

    }
}
