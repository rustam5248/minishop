package com.minishop.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// scanBasePackages picks up the shared outbox/idempotency beans from the common module
@SpringBootApplication(scanBasePackages = "com.minishop")
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
