package com.spark.tutorial.ubs;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class Person implements Serializable {

    private String name;
    private String surname;
    private int age;
    private BigDecimal salary;
    private Date bdate;

    public Person(String name, String surname, int age,BigDecimal salary,Date bdate) {
        this.name = name;
        this.surname = surname;
        this.age = age;
        this.salary=salary;
        this.bdate=bdate;
    }
    public Person(String name, String surname, int age,BigDecimal salary) {
        this.name = name;
        this.surname = surname;
        this.age = age;
        this.salary=salary;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    public Date getBdate() {
        return bdate;
    }

    public void setBdate(Date bdate) {
        this.bdate = bdate;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", age=" + age +
                '}';
    }
}
