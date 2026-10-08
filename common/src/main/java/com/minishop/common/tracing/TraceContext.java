package com.minishop.common.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Small helpers around W3C trace context. Works with the OpenTelemetry Java agent;
 * without the agent GlobalOpenTelemetry is a no-op and these methods do nothing harmful.
 */
public final class TraceContext {

    private static final String TRACEPARENT = "traceparent";

    private static final TextMapSetter<Map<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };

    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    /** A valid but NOT sampled parent. Children of it are dropped by the parent-based sampler. */
    private static final Context UNSAMPLED = Context.root().with(Span.wrap(SpanContext.create(
            "00000000000000000000000000000001", "0000000000000001",
            TraceFlags.getDefault(), TraceState.getDefault())));

    private TraceContext() {
    }

    /** e.g. "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", or null if no trace is active. */
    public static String currentTraceparent() {
        Map<String, String> carrier = new HashMap<>();
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, SETTER);
        return carrier.get(TRACEPARENT);
    }

    public static Context fromTraceparent(String traceparent) {
        if (traceparent == null) {
            return Context.root();
        }
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.root(), Map.of(TRACEPARENT, traceparent), GETTER);
    }

    /**
     * Runs background polling (outbox relay, stuck-saga query) without creating traces.
     * Otherwise every poll's SQL query would appear in Jaeger as its own trace, hiding the real ones.
     */
    public static <T> T untraced(Supplier<T> work) {
        try (Scope ignored = UNSAMPLED.makeCurrent()) {
            return work.get();
        }
    }

    public static void runUntraced(Runnable work) {
        untraced(() -> {
            work.run();
            return null;
        });
    }
}
