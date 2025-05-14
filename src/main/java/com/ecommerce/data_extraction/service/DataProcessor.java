package com.ecommerce.data_extraction.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class DataProcessor {
    private final DataSource dataSource;



    public DataProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void processWithStoredProcedure(Path csvPath, BufferedWriter errorWriter) throws IOException, SQLException {
        int batchSize = 0;
        CSVRecord recordGlobal = null;
        try (
                Reader reader = Files.newBufferedReader(csvPath);
                CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
                Connection conn = dataSource.getConnection()
        ) {

            conn.setAutoCommit(false);

            String call = "Call insert_order(?, ?, ?, ?, ?, ?, ?, ?)";
            try (CallableStatement cs = conn.prepareCall(call)) {


                int BATCH_LIMIT = 20;

                for (CSVRecord record : parser) {
                    recordGlobal = record;
                    
                    cs.setString(1, record.get("InvoiceNo"));
                    cs.setString(2, record.get("StockCode"));
                    cs.setString(3, record.get("Description"));
                    cs.setInt(4, Integer.parseInt(record.get("Quantity")));

                    LocalDateTime dateTime = LocalDateTime.parse(
                            record.get("InvoiceDate"),
                            DateTimeFormatter.ofPattern("M/d/yyyy H:mm")
                    );
                    cs.setTimestamp(5, Timestamp.valueOf(dateTime));

                    cs.setBigDecimal(6, new BigDecimal(record.get("UnitPrice")));

                    if (record.isSet("CustomerID") && !record.get("CustomerID").isEmpty()) {
                        cs.setLong(7, Long.parseLong(record.get("CustomerID")));
                    } else {
                        cs.setNull(7, Types.BIGINT);
                    }

                    cs.setString(8, record.get("Country"));


                    cs.execute();

                    if (++batchSize % BATCH_LIMIT == 0) {

                        conn.commit();
                        batchSize = 0;
                    }
                }

                cs.execute();
                conn.commit();
            }
        } catch (SQLException e) {
            System.out.println(e.getStackTrace());
            logMalformedLine(errorWriter, batchSize, e);

        } catch(UncheckedIOException e){

            System.out.println(recordGlobal);

            
            System.out.println(e.getStackTrace());
            logMalformedLine(errorWriter, batchSize, e);
        }
    }

    private void logMalformedLine(BufferedWriter writer, int lineNumber,  Exception e) {
        try {
            writer.write("⚠️ Error at line " + lineNumber + ": " + e.getClass().getSimpleName());
            writer.newLine();
            writer.write("Message: " + e.getMessage());

            writer.newLine();
            writer.write("----------------------------------------------------");
            writer.newLine();
            writer.flush(); // Ensure immediate write
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
