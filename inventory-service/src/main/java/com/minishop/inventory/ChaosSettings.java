package com.minishop.inventory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/** Fault injection switch, changeable at runtime via POST /chaos?failureRate=... */
@Component
public class ChaosSettings {

    private volatile double failureRate;

    public ChaosSettings(@Value("${minishop.chaos.failure-rate:0}") double failureRate) {
        this.failureRate = failureRate;
    }

    public void maybeFail() {
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IllegalStateException("Chaos: simulated inventory failure");
        }
    }

    public double getFailureRate() {
        return failureRate;
    }

    public void setFailureRate(double failureRate) {
        this.failureRate = failureRate;
    }
}
