package org.viktech.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.viktech.dto.OrderDto;

public class ValidateOrderProcessor implements Processor {
    @Override
    public void process(Exchange exchange) {
        OrderDto dto = exchange.getIn().getBody(OrderDto.class);
        if (dto.getOrderId() == null || dto.getOrderId().isBlank()) {
            throw new IllegalArgumentException("orderId is required");
        }
        if (dto.getAmount() == null || dto.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }
}

