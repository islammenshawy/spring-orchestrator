package com.dis.instrument.vendor.enigio;

import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.inbound.webhook.*;
import com.dis.instrument.model.FlowStep;
import com.dis.instrument.model.SigningStatus;
import com.dis.instrument.model.WebhookEvent;
import com.dis.instrument.service.NotificationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for webhook event handlers.
 * Each handler is tested in isolation with mocked MongoTemplate.
 */
@DisplayName("Webhook Event Handlers")
class WebhookHandlerTest {

    @Nested
    @DisplayName("PartiallySignedHandler")
    class PartiallySignedTests {

        @Test
        @DisplayName("increments signature count atomically")
        void incrementsSignatureCount() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new PartiallySignedHandler(mongo, notifier, reactivator);

            assertEquals(Set.of(WebhookEvent.PARTIALLY_SIGNED), handler.getSupportedEvents());

            EnigioInstrumentEntity flow = new EnigioInstrumentEntity();
            flow.setId("flow-1");
            flow.setSignaturesReceived(1);
            flow.setSignaturesRequired(3);

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(flow);

            handler.handle("trace-123", Map.of());

            verify(notifier).notifyPhaseComplete(eq(flow), anyString(), anyString());
            verify(reactivator, never()).reactivate(anyString(), anyString()); // not all signed yet
        }

        @Test
        @DisplayName("reactivates flow when all signatures received")
        void reactivatesWhenAllSigned() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new PartiallySignedHandler(mongo, notifier, reactivator);

            EnigioInstrumentEntity flow = new EnigioInstrumentEntity();
            flow.setId("flow-1");
            flow.setSignaturesReceived(3);
            flow.setSignaturesRequired(3);

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(flow);

            handler.handle("trace-123", Map.of());

            verify(reactivator).reactivate("flow-1", FlowStep.AWAIT_SIGNATURES.name());
        }

        @Test
        @DisplayName("ignores when already fully signed (guard returns null)")
        void ignoresWhenAlreadySigned() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new PartiallySignedHandler(mongo, notifier, reactivator);

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(null); // guard rejected — already at/above required

            handler.handle("trace-123", Map.of());

            verify(notifier, never()).notifyPhaseComplete(any(), anyString(), anyString());
            verify(reactivator, never()).reactivate(anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("FullySignedHandler")
    class FullySignedTests {

        @Test
        @DisplayName("marks SIGNED and reactivates flow")
        void marksSigned() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new FullySignedHandler(mongo, notifier, reactivator);

            assertEquals(Set.of(WebhookEvent.FULLY_SIGNED), handler.getSupportedEvents());

            EnigioInstrumentEntity flow = new EnigioInstrumentEntity();
            flow.setId("flow-1");

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(flow);

            handler.handle("trace-123", Map.of());

            verify(notifier).notifyPhaseComplete(eq(flow), anyString(), anyString());
            verify(reactivator).reactivate("flow-1", FlowStep.AWAIT_SIGNATURES.name());
        }

        @Test
        @DisplayName("ignores duplicate FULLY_SIGNED (already SIGNED)")
        void ignoresDuplicate() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new FullySignedHandler(mongo, notifier, reactivator);

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(null); // already SIGNED

            handler.handle("trace-123", Map.of());

            verify(notifier, never()).notifyPhaseComplete(any(), anyString(), anyString());
            verify(reactivator, never()).reactivate(anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("SignatureRejectedHandler")
    class SignatureRejectedTests {

        @Test
        @DisplayName("rejects signing with guard — does not overwrite SIGNED")
        void doesNotOverwriteSigned() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            var handler = new SignatureRejectedHandler(mongo, notifier);

            assertEquals(Set.of(WebhookEvent.SIGNATURE_REJECTED), handler.getSupportedEvents());

            // Guard returns null — already SIGNED
            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(null);

            handler.handle("trace-123", Map.of());

            verify(notifier, never()).notifyPhaseComplete(any(), anyString(), anyString());
        }

        @Test
        @DisplayName("rejects when not yet signed")
        void rejectsWhenNotSigned() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            NotificationService notifier = mock(NotificationService.class);
            var handler = new SignatureRejectedHandler(mongo, notifier);

            EnigioInstrumentEntity flow = new EnigioInstrumentEntity();
            flow.setId("flow-1");

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(flow);

            handler.handle("trace-123", Map.of());

            verify(notifier).notifyPhaseComplete(eq(flow), eq("SIGNATURE_REJECTED"), eq("REJECTED"));
        }
    }

    @Nested
    @DisplayName("TransferHandler")
    class TransferTests {

        @Test
        @DisplayName("accepts transfer and reactivates flow")
        void acceptsTransfer() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new TransferHandler(mongo, reactivator);

            assertEquals(Set.of(WebhookEvent.TRANSFER), handler.getSupportedEvents());

            EnigioInstrumentEntity flow = new EnigioInstrumentEntity();
            flow.setId("flow-1");

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(flow);

            handler.handle("envelope-trace-123", Map.of());

            verify(reactivator).reactivate("flow-1", FlowStep.TRANSFER_DOCUMENT.name());
        }

        @Test
        @DisplayName("ignores transfer for unknown envelope")
        void ignoresUnknownEnvelope() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            FlowReactivator reactivator = mock(FlowReactivator.class);
            var handler = new TransferHandler(mongo, reactivator);

            when(mongo.findAndModify(any(Query.class), any(Update.class),
                    any(FindAndModifyOptions.class), eq(EnigioInstrumentEntity.class)))
                    .thenReturn(null);

            handler.handle("unknown-envelope", Map.of());

            verify(reactivator, never()).reactivate(anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("AuditEventHandler")
    class AuditTests {

        @Test
        @DisplayName("handles all audit event types")
        void handlesAllAuditTypes() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            var handler = new AuditEventHandler(mongo);

            assertEquals(Set.of(WebhookEvent.CREATE, WebhookEvent.AMENDMENT,
                    WebhookEvent.INVALIDATE, WebhookEvent.TRANSFER_CANCELLED),
                    handler.getSupportedEvents());
        }

        @Test
        @DisplayName("CREATE sets vendorCreateConfirmed flag")
        void createSetsFlag() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            var handler = new AuditEventHandler(mongo);

            handler.handle("trace-123", Map.of("eventType", "CREATE"));

            verify(mongo).updateFirst(any(Query.class), any(Update.class), eq(EnigioInstrumentEntity.class));
        }

        @Test
        @DisplayName("INVALIDATE is log-only — no DB update")
        void invalidateLogOnly() {
            MongoTemplate mongo = mock(MongoTemplate.class);
            var handler = new AuditEventHandler(mongo);

            handler.handle("trace-123", Map.of("eventType", "INVALIDATE"));

            verify(mongo, never()).updateFirst(any(Query.class), any(Update.class), eq(EnigioInstrumentEntity.class));
        }
    }
}
