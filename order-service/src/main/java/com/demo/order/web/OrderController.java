package com.demo.order.web;

import com.demo.order.OrderEntity;
import com.demo.order.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    public record CreateOrderRequest(
            @NotBlank String customerId,
            @NotNull @DecimalMin("0.01") BigDecimal totalAmount) {}

    public record OrderResponse(
            String id,
            String customerId,
            BigDecimal totalAmount,
            String status,
            Instant createdAt,
            Instant cancelledAt) {

        static OrderResponse from(OrderEntity order) {
            return new OrderResponse(
                    order.getId(),
                    order.getCustomerId(),
                    order.getTotalAmount(),
                    order.getStatus().name(),
                    order.getCreatedAt(),
                    order.getCancelledAt());
        }
    }

    /**
     * Returns 201 as soon as the transaction commits. Publication to Kafka happens afterwards, so a
     * successful response means "durably recorded and guaranteed to be published", not "already on
     * the topic". That distinction is the whole point of the outbox.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
        return OrderResponse.from(
                orderService.createOrder(request.customerId(), request.totalAmount()));
    }

    @PostMapping("/{orderId}/cancel")
    public OrderResponse cancel(
            @PathVariable String orderId,
            @RequestParam(defaultValue = "customer request") String reason) {
        return OrderResponse.from(orderService.cancelOrder(orderId, reason));
    }

    @GetMapping
    public List<OrderResponse> list() {
        return orderService.findAll().stream().map(OrderResponse::from).toList();
    }

    @GetMapping("/{orderId}")
    public OrderResponse get(@PathVariable String orderId) {
        return OrderResponse.from(orderService.findById(orderId));
    }
}
