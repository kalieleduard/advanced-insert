package br.com.example.advancedinsert;

import br.com.example.advancedinsert.dao.AdvancedInsertService;

public class Main {
    public static void main(String[] args) {
        final var advancedInsertService = new AdvancedInsertService();
        final var largeTables = advancedInsertService.searchAll();
        advancedInsertService.insertIntoMySQL(largeTables);
    }
}