package com.ecommerce.data_extraction;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class DataExtractionApplicationTests {

        @Autowired
        private RequestMappingHandlerMapping handlerMapping;

        @Test
        void contextLoads() {
        }

        @Test
        void uploadEndpointMapped() {
                boolean exists = handlerMapping.getHandlerMethods().keySet().stream()
                        .flatMap(info -> info.getPathPatternsCondition().getPatternValues().stream())
                        .anyMatch(p -> p.equals("/process-data/upload"));
                assertTrue(exists, "upload endpoint mapping missing");
        }

}
