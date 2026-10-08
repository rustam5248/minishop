package com.minishop.payment.bank;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

/**
 * Synchronous call to an external provider, protected by:
 *  - connect/read timeouts (RestClient request factory)
 *  - @Retry with exponential backoff + jitter, only for transient errors
 *  - @CircuitBreaker so a dead bank makes us fail fast instead of piling up waiting threads
 * Aspect order (Resilience4j default): Retry( CircuitBreaker( call ) ).
 *
 * Retrying a payment is only safe because of the Idempotency-Key header: the bank returns
 * the original result for a repeated key instead of charging twice.
 */
@Component
public class BankClient {

    private final RestClient rest;

    public BankClient(RestClient.Builder builder,
                      @Value("${minishop.bank.url}") String baseUrl,
                      @Value("${minishop.bank.connect-timeout-ms:1000}") long connectTimeoutMs,
                      @Value("${minishop.bank.read-timeout-ms:2000}") long readTimeoutMs) {
        var requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        this.rest = builder.baseUrl(baseUrl).requestFactory(requestFactory).build();
    }

    @Retry(name = "bank")
    @CircuitBreaker(name = "bank")
    @WithSpan("bank.charge")   // one span per attempt, so retries are visible in Jaeger
    public String charge(UUID orderId, BigDecimal amount) {
        Span.current().setAttribute("order.id", orderId.toString());
        try {
            ChargeResponse response = rest.post()
                    .uri("/charges")
                    .header("Idempotency-Key", orderId.toString())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new ChargeRequest(amount))
                    .retrieve()
                    .body(ChargeResponse.class);
            if (response == null || response.transactionId() == null) {
                throw new IllegalStateException("Bank returned an empty response");
            }
            return response.transactionId();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 402) {
                throw new PaymentDeclinedException(e.getResponseBodyAsString());
            }
            throw e;
        }
    }

    public record ChargeRequest(BigDecimal amount) {
    }

    public record ChargeResponse(String transactionId, String status, String reason) {
    }
}
