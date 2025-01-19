package br.com.example.advancedinsert.dao.entity;

import java.time.LocalDate;
import java.util.UUID;

public record LargeTable(
        UUID id,
        LocalDate date
) {
}
