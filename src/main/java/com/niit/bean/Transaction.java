package com.niit.bean;

public class Transaction {
    public String userId;
    public Double amount;

    public Transaction(String userId, Double amount) {
        this.userId = userId;
        this.amount = amount;
    }
}
