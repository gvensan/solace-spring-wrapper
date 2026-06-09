package com.solace.wrapper.consumer;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SolaceConsumerRegistry}, verifying it is a thin, faithful delegate over
 * {@link SolaceConsumerManager}.
 */
class SolaceConsumerRegistryTest {

    @Test
    void delegates_consumer_ids() {
        SolaceConsumerManager manager = mock(SolaceConsumerManager.class);
        Set<String> ids = Set.of("a", "b");
        when(manager.getConsumerIds()).thenReturn(ids);

        SolaceConsumerRegistry registry = new SolaceConsumerRegistry(manager);

        assertThat(registry.getConsumerIds()).isEqualTo(ids);
        verify(manager).getConsumerIds();
    }

    @Test
    void delegates_all_statuses() {
        SolaceConsumerManager manager = mock(SolaceConsumerManager.class);
        SolaceConsumerManager.ConsumerStatus status = new SolaceConsumerManager.ConsumerStatus(
                "c1", "q1", new String[]{"t/1"}, "PERSISTENT", "String", true, false);
        Map<String, SolaceConsumerManager.ConsumerStatus> map = Map.of("c1", status);
        when(manager.getAllConsumerStatuses()).thenReturn(map);

        SolaceConsumerRegistry registry = new SolaceConsumerRegistry(manager);

        assertThat(registry.getConsumerStatuses()).containsEntry("c1", status);
        verify(manager).getAllConsumerStatuses();
    }

    @Test
    void delegates_single_status_including_null() {
        SolaceConsumerManager manager = mock(SolaceConsumerManager.class);
        SolaceConsumerManager.ConsumerStatus status = new SolaceConsumerManager.ConsumerStatus(
                "c1", "q1", new String[0], "DIRECT", "String", true, false);
        when(manager.getConsumerStatus("c1")).thenReturn(status);
        when(manager.getConsumerStatus("missing")).thenReturn(null);

        SolaceConsumerRegistry registry = new SolaceConsumerRegistry(manager);

        assertThat(registry.getConsumerStatus("c1")).isSameAs(status);
        assertThat(registry.getConsumerStatus("missing")).isNull();
    }
}
