package com.demo.order.web;

import com.demo.events.OrderCreated;
import com.demo.order.OrderService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Endpoints that exist purely to make the guarantees observable.
 *
 * <p>Not something to ship — it deliberately triggers a condition that is otherwise rare and
 * timing-dependent, so the inbox can be watched doing its job on demand.
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    private final OrderService orderService;

    public DemoController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Forces a duplicate publication of an order's original OrderCreated event.
     *
     * <p>Expected outcome downstream: no new shipment, no new inbox row, and
     * {@code inbox_messages_duplicate_total} incremented by one.
     */
    @PostMapping("/duplicate/{orderId}")
    public Map<String, Object> forgeDuplicate(@PathVariable String orderId) {
        OrderCreated duplicate = orderService.replayCreatedEvent(orderId);
        return Map.of(
                "orderId", orderId,
                "replayedEventId", duplicate.eventId(),
                "expectation",
                        "shipping-service must suppress this as a duplicate; "
                                + "check GET /inbox on port 8091 and the inbox.messages.duplicate counter");
    }
}
