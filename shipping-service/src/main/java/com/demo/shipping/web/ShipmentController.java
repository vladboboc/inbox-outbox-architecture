package com.demo.shipping.web;

import com.demo.inbox.InboxMessage;
import com.demo.inbox.InboxMessageRepository;
import com.demo.shipping.Shipment;
import com.demo.shipping.ShipmentService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Read-only views that make the demo verifiable without opening a psql session. */
@RestController
public class ShipmentController {

    private final ShipmentService shipmentService;
    private final InboxMessageRepository inboxMessageRepository;

    public ShipmentController(
            ShipmentService shipmentService, InboxMessageRepository inboxMessageRepository) {
        this.shipmentService = shipmentService;
        this.inboxMessageRepository = inboxMessageRepository;
    }

    public record ShipmentResponse(
            String id,
            String orderId,
            String customerId,
            BigDecimal orderTotal,
            String status,
            Instant createdAt) {

        static ShipmentResponse from(Shipment shipment) {
            return new ShipmentResponse(
                    shipment.getId(),
                    shipment.getOrderId(),
                    shipment.getCustomerId(),
                    shipment.getOrderTotal(),
                    shipment.getStatus().name(),
                    shipment.getCreatedAt());
        }
    }

    public record InboxEntryResponse(
            String eventId,
            String consumer,
            String topic,
            int partition,
            long offset,
            Instant receivedAt) {

        static InboxEntryResponse from(InboxMessage message) {
            return new InboxEntryResponse(
                    message.getEventId(),
                    message.getConsumer(),
                    message.getTopic(),
                    message.getPartitionNo(),
                    message.getRecordOffset(),
                    message.getReceivedAt());
        }
    }

    @GetMapping("/shipments")
    public List<ShipmentResponse> shipments() {
        return shipmentService.findAll().stream().map(ShipmentResponse::from).toList();
    }

    /**
     * The dedup ledger. Comparing its size against {@code /shipments} is the quickest way to see the
     * inbox working: replaying an event leaves both counts unchanged.
     */
    @GetMapping("/inbox")
    public Map<String, Object> inbox() {
        List<InboxEntryResponse> entries =
                inboxMessageRepository.findTop100ByOrderByReceivedAtDesc().stream()
                        .map(InboxEntryResponse::from)
                        .toList();

        return Map.of(
                "totalClaims", inboxMessageRepository.count(),
                "shipments", shipmentService.findAll().size(),
                "recent", entries);
    }
}
