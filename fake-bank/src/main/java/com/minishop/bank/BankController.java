package com.minishop.bank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mock payment provider that behaves like real ones (Stripe, Adyen...):
 *  - Idempotency-Key: the same key always returns the same result, it never charges twice
 *  - amounts above 1000 are declined with HTTP 402 (deterministic compensation test)
 *  - chaos switches: artificial latency and random 503s, changeable at runtime
 */
@RestController
public class BankController {

    private static final Logger log = LoggerFactory.getLogger(BankController.class);
    private static final BigDecimal LIMIT = new BigDecimal("1000");

    private final Map<String, Charge> charges = new ConcurrentHashMap<>();
    private volatile long delayMs;
    private volatile double failureRate;

    public BankController(@Value("${bank.delay-ms:0}") long delayMs,
                          @Value("${bank.failure-rate:0}") double failureRate) {
        this.delayMs = delayMs;
        this.failureRate = failureRate;
    }

    @PostMapping("/charges")
    public ResponseEntity<Charge> charge(@RequestHeader("Idempotency-Key") String key,
                                         @RequestBody ChargeRequest request) throws InterruptedException {
        if (delayMs > 0) {
            Thread.sleep(delayMs);
        }
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            log.warn("Simulated outage for key {}", key);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        boolean[] replay = {true};
        Charge charge = charges.computeIfAbsent(key, k -> {
            replay[0] = false;
            return decide(request);
        });
        log.info("{} key={} amount={} -> {}", replay[0] ? "Idempotent replay" : "New charge",
                key, request.amount(), charge.status());
        return "APPROVED".equals(charge.status())
                ? ResponseEntity.ok(charge)
                : ResponseEntity.status(HttpStatus.PAYMENT_REQUIRED).body(charge);
    }

    /** Lets you check what the bank really did, e.g. after a client-side timeout. */
    @GetMapping("/charges/{key}")
    public ResponseEntity<Charge> find(@PathVariable String key) {
        return ResponseEntity.ofNullable(charges.get(key));
    }

    @GetMapping("/chaos")
    public Map<String, Object> chaos() {
        return Map.of("delayMs", delayMs, "failureRate", failureRate);
    }

    @PostMapping("/chaos")
    public Map<String, Object> setChaos(@RequestParam(required = false) Long delayMs,
                                        @RequestParam(required = false) Double failureRate) {
        if (delayMs != null) {
            this.delayMs = delayMs;
        }
        if (failureRate != null) {
            this.failureRate = failureRate;
        }
        return chaos();
    }

    private Charge decide(ChargeRequest request) {
        if (request.amount() == null || request.amount().compareTo(LIMIT) > 0) {
            return new Charge(null, "DECLINED", "Amount exceeds limit of " + LIMIT);
        }
        return new Charge("bank-tx-" + UUID.randomUUID(), "APPROVED", null);
    }

    public record ChargeRequest(BigDecimal amount) {
    }

    public record Charge(String transactionId, String status, String reason) {
    }
}
