package com.demo.shipping;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ShipmentRepository extends JpaRepository<Shipment, String> {

    List<Shipment> findByOrderId(String orderId);

    Optional<Shipment> findFirstByOrderId(String orderId);
}
