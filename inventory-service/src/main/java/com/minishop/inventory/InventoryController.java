package com.minishop.inventory;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class InventoryController {

    private final JdbcTemplate jdbc;
    private final ChaosSettings chaos;

    public InventoryController(JdbcTemplate jdbc, ChaosSettings chaos) {
        this.jdbc = jdbc;
        this.chaos = chaos;
    }

    @GetMapping("/products")
    public List<Map<String, Object>> products() {
        return jdbc.queryForList("SELECT id, name, available FROM products ORDER BY id");
    }

    @GetMapping("/chaos")
    public Map<String, Object> chaos() {
        return Map.of("failureRate", chaos.getFailureRate());
    }

    @PostMapping("/chaos")
    public Map<String, Object> setChaos(@RequestParam double failureRate) {
        chaos.setFailureRate(failureRate);
        return Map.of("failureRate", failureRate);
    }
}
