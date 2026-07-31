package com.demo.shipping;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Business reaction to order events.
 *
 * <p>{@link Propagation#MANDATORY} on the mutating methods enforces the invariant the whole design
 * rests on: this work must commit in the same transaction as the inbox claim that authorised it.
 * Running standalone would reintroduce the very gap the inbox closes.
 */
@Service
public class ShipmentService {

    private static final Logger log = LoggerFactory.getLogger(ShipmentService.class);

    /**
     * Customer id that makes handling fail on purpose, to exercise retry and the dead-letter path.
     */
    static final String POISON_CUSTOMER_ID = "CUST-POISON";

    private final ShipmentRepository shipmentRepository;

    public ShipmentService(ShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Shipment createShipment(OrderCreated event) {
        if (POISON_CUSTOMER_ID.equals(event.customerId())) {
            // Retried by the container's error handler, then routed to orders.v1.DLT. The inbox
            // claim rolls back with this exception, so the redeliveries are genuine first attempts
            // rather than being mistaken for duplicates.
            throw new IllegalStateException(
                    "poison event for customer " + POISON_CUSTOMER_ID + " (deliberate demo failure)");
        }

        Shipment shipment =
                shipmentRepository.save(
                        new Shipment(event.orderId(), event.customerId(), event.totalAmount()));

        log.info(
                "shipment created id={} order={} customer={}",
                shipment.getId(),
                event.orderId(),
                event.customerId());
        return shipment;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void cancelShipment(OrderCancelled event) {
        shipmentRepository
                .findFirstByOrderId(event.orderId())
                .ifPresentOrElse(
                        shipment -> {
                            shipment.cancel();
                            shipmentRepository.save(shipment);
                            log.info(
                                    "shipment cancelled id={} order={} reason={}",
                                    shipment.getId(),
                                    event.orderId(),
                                    event.reason());
                        },
                        () ->
                                // Not an error: cancellation can legitimately arrive before the
                                // creation event has been consumed.
                                log.warn(
                                        "cancellation for order={} has no shipment yet; nothing to do",
                                        event.orderId()));
    }

    @Transactional(readOnly = true)
    public List<Shipment> findAll() {
        return shipmentRepository.findAll();
    }
}
