package br.com.example.advancedinsert.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dummy")
public class DummyController {

    @GetMapping
    public ResponseEntity<String> dummy() {
        return new ResponseEntity<>("Everything is OK!", HttpStatus.OK);
    }
}
