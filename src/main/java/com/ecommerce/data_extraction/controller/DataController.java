package com.ecommerce.data_extraction.controller;

import com.ecommerce.data_extraction.service.DataProcessor;
import com.ecommerce.data_extraction.service.DataProcessorHibernate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@RestController
@RequestMapping("/process-data")
public class DataController {

    private final DataProcessor dataProcessor;
    private final DataProcessorHibernate dataProcessorHibernate;


    public DataController(DataProcessor dataProcessor, DataProcessorHibernate dataProcessorHibernate) {
        this.dataProcessor = dataProcessor;
        this.dataProcessorHibernate = dataProcessorHibernate;
    }

    @PostMapping("/jdbc")
    public ResponseEntity<String> importData() {
        try {
            Path path = Paths.get("../src/main/resources/data.csv");
            BufferedWriter errorWriter = Files.newBufferedWriter(
                    Paths.get("../src/main/resources/error_log.txt"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            dataProcessor.processWithStoredProcedure(path, errorWriter);
            dataProcessorHibernate.importCSV(path);
            return ResponseEntity.ok("✅ Stored procedure import successful!");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("❌ Import failed: " + e.getMessage());
        }
    }

    @PostMapping("/hibernate")
    public ResponseEntity<String> importDataHibernate() {
        try {
            Path path = Paths.get("/Users/aakashbasnet/Projects/DataProcessing/data-extraction/src/main/resources/data.csv");

            dataProcessorHibernate.importCSV(path);
            return ResponseEntity.ok("✅ Stored procedure import successful!");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("❌ Import failed: " + e.getMessage());
        }
    }
}
