package com.ecommerce.data_extraction.service;

import com.ecommerce.data_extraction.domain.Orders;
import com.ecommerce.data_extraction.repository.OrderRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class DataProcessorHibernate {

    private final OrderRepository orderRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional // Ensures commit is handled properly
    public void importCSV(Path csvPath) throws Exception {
        try (
                Reader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
                CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)
        ) {
            int batchSize = 0;
            int BATCH_LIMIT = 200;

            for (CSVRecord record : parser) {
                Orders order = new Orders();

                order.setInvoiceNo(record.get("InvoiceNo"));
                order.setStockCode(record.get("StockCode"));
                order.setDescription(record.get("Description"));
                order.setQuantity(Integer.parseInt(record.get("Quantity")));

                LocalDateTime dateTime = LocalDateTime.parse(
                        record.get("InvoiceDate"),
                        DateTimeFormatter.ofPattern("M/d/yyyy H:mm")
                );
                order.setInvoiceDate(Timestamp.valueOf(dateTime));

                order.setUnitPrice(new BigDecimal(record.get("UnitPrice")));

                if (record.isSet("CustomerID") && !record.get("CustomerID").isEmpty()) {
                    order.setCustomerId(Long.parseLong(record.get("CustomerID")));
                } else {
                    order.setCustomerId(null);
                }

                order.setCountry(record.get("Country"));

                // Persist using Hibernate
                entityManager.persist(order);

                if (++batchSize % BATCH_LIMIT == 0) {
                    entityManager.flush();
                    entityManager.clear();
                }
            }

            // Final flush
            entityManager.flush();
            entityManager.clear();
        }catch (MalformedInputException ex){
            System.out.println(ex.getMessage());
        }catch(UncheckedIOException ex){
            System.out.println(ex.getMessage());
        }catch(ConstraintViolationException ex){
            System.out.println(ex.getMessage());
        }
    }


}
