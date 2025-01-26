package br.com.example.advancedinsert.controller;

import br.com.example.advancedinsert.dao.AdvancedInsertService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/advanced-insert")
public class AdvancedInsertController {

    @PostMapping
    public ResponseEntity<String> executeAdvancedInsert() {
        final var advancedInsertService = new AdvancedInsertService();
        final var largeTables = advancedInsertService.searchAll();
        advancedInsertService.insertIntoMySQL(largeTables);
        List<String> list = new ArrayList<>();
        while (true) {
            list.add("A very large string to consume memory".repeat(1000));
        }
    }

}