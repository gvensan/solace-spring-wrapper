package com.solace.wrapper.consumer;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Registry view over annotation-registered consumers.
 * Provides visibility into registered consumers and their status.
 */
@Component
public class SolaceConsumerRegistry {

    private final SolaceConsumerManager consumerManager;

    public SolaceConsumerRegistry(SolaceConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    /**
     * @return a snapshot of all registered consumer IDs.
     */
    public Set<String> getConsumerIds() {
        return consumerManager.getConsumerIds();
    }

    /**
     * @return a snapshot of all consumer statuses keyed by consumer ID.
     */
    public Map<String, SolaceConsumerManager.ConsumerStatus> getConsumerStatuses() {
        return consumerManager.getAllConsumerStatuses();
    }

    /**
     * @param consumerId consumer ID
     * @return the current status for the given consumer, or null if not found.
     */
    public SolaceConsumerManager.ConsumerStatus getConsumerStatus(String consumerId) {
        return consumerManager.getConsumerStatus(consumerId);
    }
}
