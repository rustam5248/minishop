package com.minishop.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Order(UUID id,
                    String customerId,
                    String productId,
                    int quantity,
                    BigDecimal amount,
                    String status,
                    String rejectReason,
                    Instant createdAt,
                    Instant updatedAt) {
}
