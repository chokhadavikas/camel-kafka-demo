package org.viktech.routes;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.viktech.dto.OrderDto;
import org.viktech.processor.ValidateOrderProcessor;

@Component
public class OrderRoutes extends RouteBuilder {

    @Value("${app.topics.orders}")
    String ordersTopic;

    @Value("${app.topics.ordersDlq}")
    String ordersDlqTopic;

    @Override
    public void configure() {

        // Global error handler for unmatched exceptions
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(0));

        // Expose REST endpoint using Camel REST DSL (backed by Spring Boot servlet)
        restConfiguration()
                .component("servlet")
                .contextPath("/")
        //        .apiContextPath("/api-doc") // optional
          //      .apiProperty("api.title", "Camel Kafka Demo")
            //    .apiProperty("api.version", "1.0")
        ;

        rest("/orders")
                .post()
                .consumes("application/json")
                .produces("application/json")
                .to("direct:orderIngest");

        // Consumer route: consume from Kafka, simulate processing, retry, DLQ
        onException(Exception.class)
                .maximumRedeliveries(3)
                .redeliveryDelay(1000)
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .handled(true)
                .log(LoggingLevel.ERROR, "Processing failed after retries. Sending to DLQ: ${exception.message}")
                .toD("kafka:orders.dlq?brokers=localhost:9092");


        // Ingest route: validate and publish to Kafka
        from("direct:orderIngest")
                .routeId("order-ingest")
                .log(LoggingLevel.INFO, "Received order request")
                .unmarshal().json(OrderDto.class)
                .process(new ValidateOrderProcessor())
                .setHeader("kafka.KEY", simple("${body.orderId}"))
                .marshal().json()
                .toD("kafka:orders?brokers=localhost:9092")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
                .setBody().simple("{\"status\":\"accepted\"}");


        from("kafka:{{app.topics.orders}}?brokers=localhost:9092&groupId=camel-order-consumer&autoOffsetReset=earliest")
                .routeId("order-consumer")
                .log(LoggingLevel.INFO, "Consumed from Kafka key=${header[kafka.KEY]} value=${body}")
                .unmarshal().json(OrderDto.class)
                // Simulate business rule; fail certain orders to see retry/DLQ
                .choice()
                .when(simple("${body.amount} > 1000"))
                .log(LoggingLevel.INFO, "High-value order: ${body.orderId}")
                .otherwise()
                .log(LoggingLevel.INFO, "Normal order: ${body.orderId}")
                .end()
                // Uncomment to simulate an error:
                // .process(e -> { throw new RuntimeException("Simulated failure"); })
                .log(LoggingLevel.INFO, "Processed order ${body.orderId}");
    }
}

