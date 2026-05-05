package com.dis.instrument.vendor.enigio;

import com.dis.instrument.service.NotificationService;
import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.dis.instrument.flow.EnigioInstrumentFlow;
import com.dis.instrument.vendor.enigio.EnigioClient;
import com.dis.instrument.model.*;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.exception.NonRetryableStepException;
import com.orchestrator.starter.exception.RetryableStepException;
import com.orchestrator.starter.exception.WaitingStepException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("EnigioInstrumentFlow step logic")
class EnigioInstrumentFlowTest {

    @Mock
    private EnigioClient enigioClient;

    @Mock
    private AdditionalDocumentRepository additionalDocumentRepository;

    @Mock
    private NotificationService notificationPublisher;

    @Mock
    private OrchestratorFlowRepository repository;

    private EnigioInstrumentFlow flow;

    @BeforeEach
    void setUp() {
        flow = new EnigioInstrumentFlow(enigioClient, additionalDocumentRepository,
                notificationPublisher, 48, 72,
                "http://localhost:8087/webhooks/enigio");
        ReflectionTestUtils.setField(flow, "rawRepository", repository);
    }

    private EnigioInstrumentEntity entity() {
        return EnigioInstrumentEntityTest.buildValidEntity();
    }

    @Nested
    @DisplayName("Group 1 — Document Preparation")
    class Group1 {

        @Test
        @DisplayName("step 1: createDraft sets pdfGenerated")
        void createDraft() {
            var e = entity();
            flow.createDraft(e);
            assertThat(e.isPdfGenerated()).isTrue();
        }

        @Test
        @DisplayName("step 2: registerDocument calls Enigio and saves traceOriginalId")
        void registerDocument() {
            var e = entity();
            e.setPdfGenerated(true);

            when(enigioClient.createDocument(anyString(), anyString(), anyString(), anyMap(), any()))
                    .thenReturn(new EnigioClient.DocumentResponse("to_abc123", "v1_key"));

            flow.registerDocument(e);

            assertThat(e.getTraceOriginalId()).isEqualTo("to_abc123");
            assertThat(e.getVersionKey()).isEqualTo("v1_key");
            verify(enigioClient).createDocument(
                    eq("INV-2026-001"), eq("Promissory note"), eq("NEG"), anyMap(), any());
        }

        @Test
        @DisplayName("step 3: addAttachment calls amend and saves versionKey")
        void addAttachment() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setVersionKey("v1_key");
            e.setAttachments(List.of(new Attachment("terms.pdf", "base64data", "Terms sheet")));

            when(enigioClient.amendDocument(anyString(), anyString(), anyMap(), anyList()))
                    .thenReturn(new EnigioClient.DocumentResponse("to_abc123", "v2_key"));

            flow.addAttachment(e);

            assertThat(e.getAttachmentVersionKey()).isEqualTo("v2_key");
            assertThat(e.getVersionKey()).isEqualTo("v2_key");
        }

        @Test
        @DisplayName("step 3: skips when no attachments")
        void addAttachmentSkipsWhenEmpty() {
            var e = entity();
            e.setAttachments(null);

            flow.addAttachment(e);

            assertThat(e.getAttachmentVersionKey()).isEqualTo("NONE");
            verifyNoInteractions(enigioClient);
        }
    }

    @Nested
    @DisplayName("Gate 1 — Preparation Approval")
    class Gate1 {

        @Test
        @DisplayName("publishes notification and throws retryable when not approved")
        void awaitApproval_notApproved() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            assertThatThrownBy(() -> flow.awaitPreparationApproval(e))
                    .isInstanceOf(WaitingStepException.class)
                    .hasMessageContaining("approve");

            assertThat(e.isPreparationNotified()).isTrue();
            verify(notificationPublisher).notifyPhaseComplete(eq(e),
                    eq("PREPARATION_COMPLETE"), eq("AWAITING_APPROVAL"));
        }

        @Test
        @DisplayName("advances when approved")
        void awaitApproval_approved() {
            var e = entity();
            e.setPreparationNotified(true);
            e.setSigningApproved(true);

            flow.awaitPreparationApproval(e);
            // no exception = success
        }

        @Test
        @DisplayName("does not re-publish notification on retry")
        void awaitApproval_noDoubleNotify() {
            var e = entity();
            e.setPreparationNotified(true);
            e.setPreparationNotifiedAt(java.time.Instant.now());
            e.setSigningApproved(false);

            assertThatThrownBy(() -> flow.awaitPreparationApproval(e))
                    .isInstanceOf(WaitingStepException.class);

            verifyNoInteractions(notificationPublisher);
        }

        @Test
        @DisplayName("expires if approval not received within threshold")
        void awaitApproval_expires() {
            var e = entity();
            e.setPreparationNotified(true);
            e.setPreparationNotifiedAt(java.time.Instant.now().minus(java.time.Duration.ofHours(73)));
            e.setSigningApproved(false);

            assertThatThrownBy(() -> flow.awaitPreparationApproval(e))
                    .isInstanceOf(NonRetryableStepException.class)
                    .hasMessageContaining("expired");

            verify(notificationPublisher).notifyPhaseComplete(eq(e), eq("APPROVAL_EXPIRED"), anyString());
        }
    }

    @Nested
    @DisplayName("Group 2 — Signing Ceremony")
    class Group2 {

        @Test
        @DisplayName("step 5: addSigners calls Enigio with correct signer data")
        void addSigners() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            flow.addSigners(e);

            assertThat(e.isSignersAdded()).isTrue();
            verify(enigioClient).addRequiredSignatures(eq("to_abc123"), eq(e.getSigners()));
        }

        @Test
        @DisplayName("step 5: sendForSigning sends emails")
        void sendForSigning() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.sendSigningEmails("to_abc123", "en")).thenReturn(List.of());

            flow.sendForSigning(e);

            assertThat(e.isSigningEmailsSent()).isTrue();
        }

        @Test
        @DisplayName("step 5: retries when emails fail")
        void sendForSigningRetries() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.sendSigningEmails("to_abc123", "en"))
                    .thenReturn(List.of("jane@acme.com"));

            assertThatThrownBy(() -> flow.sendForSigning(e))
                    .isInstanceOf(RetryableStepException.class)
                    .hasMessageContaining("jane@acme.com");
        }

        @Test
        @DisplayName("step 6: sendForSigning registers webhook and sets signaturesRequired")
        void sendForSigningRegistersWebhook() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.sendSigningEmails("to_abc123", "en")).thenReturn(List.of());

            flow.sendForSigning(e);

            assertThat(e.isWebhookRegistered()).isTrue();
            assertThat(e.getSignaturesRequired()).isEqualTo(2); // 2 signers in test entity
            assertThat(e.getSigningStartedAt()).isNotNull();
            verify(enigioClient).registerWebhook(anyString(), anyList());
        }

        @Test
        @DisplayName("step 7: advances immediately when webhook already set SIGNED")
        void awaitSignatures_webhookAlreadySigned() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSigningStatus("SIGNED");
            e.setSignaturesReceived(2);
            e.setSignaturesRequired(2);

            flow.awaitSignatures(e);

            assertThat(e.getSigningStatus()).isEqualTo("SIGNED");
            verifyNoInteractions(enigioClient); // no poll needed
        }

        @Test
        @DisplayName("step 7: polls as fallback when webhook not received")
        void awaitSignatures_pollFallback() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSigningStartedAt(java.time.Instant.now());

            when(enigioClient.getSigningStatus("to_abc123")).thenReturn("PENDING");

            assertThatThrownBy(() -> flow.awaitSignatures(e))
                    .isInstanceOf(WaitingStepException.class);
            assertThat(e.getSigningStatus()).isEqualTo("PENDING");
            verify(enigioClient).getSigningStatus("to_abc123");
        }

        @Test
        @DisplayName("step 7: poll fallback detects SIGNED")
        void awaitSignatures_pollDetectsSigned() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSignaturesRequired(2);
            e.setSigningStartedAt(java.time.Instant.now());

            when(enigioClient.getSigningStatus("to_abc123")).thenReturn("SIGNED");

            flow.awaitSignatures(e);

            assertThat(e.getSigningStatus()).isEqualTo("SIGNED");
            assertThat(e.getSignaturesReceived()).isEqualTo(2);
        }

        @Test
        @DisplayName("step 7: fails permanently when REJECTED")
        void awaitSignaturesRejected() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSigningStartedAt(java.time.Instant.now());

            when(enigioClient.getSigningStatus("to_abc123")).thenReturn("REJECTED");

            assertThatThrownBy(() -> flow.awaitSignatures(e))
                    .isInstanceOf(NonRetryableStepException.class)
                    .hasMessageContaining("rejected");
        }

        @Test
        @DisplayName("step 7: expires after threshold with final poll")
        void awaitSignatures_expires() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSignaturesRequired(2);
            e.setSignaturesReceived(1);
            // Set signing started 49 hours ago (threshold is 48h)
            e.setSigningStartedAt(java.time.Instant.now().minus(java.time.Duration.ofHours(49)));

            when(enigioClient.getSigningStatus("to_abc123")).thenReturn("PENDING");

            assertThatThrownBy(() -> flow.awaitSignatures(e))
                    .isInstanceOf(NonRetryableStepException.class)
                    .hasMessageContaining("expired");

            assertThat(e.getSigningStatus()).isEqualTo("EXPIRED");
            verify(notificationPublisher).notifyPhaseComplete(eq(e), eq("SIGNING_EXPIRED"), anyString());
        }

        @Test
        @DisplayName("step 7: last-minute signing detected on expiry check")
        void awaitSignatures_lastMinuteSigning() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");
            e.setSignaturesRequired(2);
            e.setSigningStartedAt(java.time.Instant.now().minus(java.time.Duration.ofHours(49)));

            // Final poll finds it was signed just in time
            when(enigioClient.getSigningStatus("to_abc123")).thenReturn("SIGNED");

            flow.awaitSignatures(e);

            assertThat(e.getSigningStatus()).isEqualTo("SIGNED");
        }
    }

    @Nested
    @DisplayName("Gate 2 — Delivery Approval")
    class Gate2 {

        @Test
        @DisplayName("publishes notification and throws retryable when not approved")
        void awaitDeliveryApproval_notApproved() {
            var e = entity();
            e.setSigningStatus("SIGNED");

            assertThatThrownBy(() -> flow.awaitDeliveryApproval(e))
                    .isInstanceOf(WaitingStepException.class)
                    .hasMessageContaining("approve");

            assertThat(e.isSigningNotified()).isTrue();
            verify(notificationPublisher).notifyPhaseComplete(eq(e),
                    eq("SIGNING_COMPLETE"), eq("AWAITING_APPROVAL"));
        }

        @Test
        @DisplayName("advances when approved")
        void awaitDeliveryApproval_approved() {
            var e = entity();
            e.setSigningNotified(true);
            e.setDeliveryApproved(true);

            flow.awaitDeliveryApproval(e);
            // no exception = success
        }

        @Test
        @DisplayName("expires if delivery approval not received within threshold")
        void awaitDeliveryApproval_expires() {
            var e = entity();
            e.setSigningNotified(true);
            e.setSigningNotifiedAt(java.time.Instant.now().minus(java.time.Duration.ofHours(73)));
            e.setDeliveryApproved(false);

            assertThatThrownBy(() -> flow.awaitDeliveryApproval(e))
                    .isInstanceOf(NonRetryableStepException.class)
                    .hasMessageContaining("expired");

            verify(notificationPublisher).notifyPhaseComplete(eq(e), eq("APPROVAL_EXPIRED"), anyString());
        }
    }

    @Nested
    @DisplayName("Group 3 — Packaging & Delivery")
    class Group3 {

        @Test
        @DisplayName("step 9: validateDocument succeeds when VALID")
        void validateValid() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.validateDocument("to_abc123")).thenReturn("VALID");

            flow.validateDocument(e);

            assertThat(e.getValidationResult()).isEqualTo("VALID");
        }

        @Test
        @DisplayName("step 7: fails permanently when NOT_VALID")
        void validateNotValid() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.validateDocument("to_abc123")).thenReturn("NOT_VALID");

            assertThatThrownBy(() -> flow.validateDocument(e))
                    .isInstanceOf(NonRetryableStepException.class);
        }

        @Test
        @DisplayName("step 8: createEnvelope creates draft and seals")
        void createEnvelope() {
            var e = entity();
            e.setTraceOriginalId("to_abc123");

            when(enigioClient.createEnvelopeDraft(anyString(), anyString(), eq("to_abc123")))
                    .thenReturn("draft_xyz");
            when(enigioClient.sealEnvelopeDraft("draft_xyz"))
                    .thenReturn(new EnigioClient.DocumentResponse("to_env_sealed", "env_v1"));

            flow.createEnvelope(e);

            assertThat(e.getEnvelopeDraftId()).isEqualTo("draft_xyz");
            assertThat(e.getEnvelopeTraceId()).isEqualTo("to_env_sealed");
            assertThat(e.getEnvelopeVersionKey()).isEqualTo("env_v1");
        }

        @Test
        @DisplayName("step 9: transferDocument initiates transfer then waits")
        void transferDocument_initiatesAndWaits() {
            var e = entity();
            e.setEnvelopeTraceId("to_env_sealed");
            e.setEnvelopeVersionKey("env_v1");

            when(enigioClient.transferByEmail(anyString(), anyString(), anyString(),
                    anyString(), anyString(), anyString()))
                    .thenReturn("xfr_001");

            assertThatThrownBy(() -> flow.transferDocument(e))
                    .isInstanceOf(WaitingStepException.class);

            assertThat(e.getTransferId()).isEqualTo("xfr_001");
            assertThat(e.getTransferInitiatedAt()).isNotNull();
        }

        @Test
        @DisplayName("step 9: transferDocument completes when recipient accepts")
        void transferDocument_completesOnAcceptance() {
            var e = entity();
            e.setEnvelopeTraceId("to_env_sealed");
            e.setEnvelopeVersionKey("env_v1");
            e.setTransferId("xfr_001");
            e.setTransferAccepted(true);

            flow.transferDocument(e);
            // No exception = step completed
            verify(notificationPublisher).notifyPhaseComplete(eq(e),
                    eq("FLOW_COMPLETE"), eq("COMPLETED"));
        }

        @Test
        @DisplayName("step 9: transferDocument fails when recipient rejects")
        void transferDocument_failsOnRejection() {
            var e = entity();
            e.setEnvelopeTraceId("to_env_sealed");
            e.setEnvelopeVersionKey("env_v1");
            e.setTransferId("xfr_001");
            e.setTransferRejected(true);

            assertThatThrownBy(() -> flow.transferDocument(e))
                    .isInstanceOf(NonRetryableStepException.class)
                    .hasMessageContaining("rejected");
        }
    }
}
