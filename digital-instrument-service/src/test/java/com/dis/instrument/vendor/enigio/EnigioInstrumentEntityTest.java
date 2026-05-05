package com.dis.instrument.vendor.enigio;

import com.dis.instrument.model.*;
import com.dis.instrument.flow.EnigioInstrumentEntity;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EnigioInstrumentEntity validation")
class EnigioInstrumentEntityTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    @DisplayName("valid entity passes validation")
    void validEntity() {
        EnigioInstrumentEntity entity = buildValidEntity();
        Set<ConstraintViolation<EnigioInstrumentEntity>> violations = validator.validate(entity);
        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("missing reference fails validation")
    void missingReference() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setReference(null);
        assertViolation(entity, "reference");
    }

    @Test
    @DisplayName("missing title fails validation")
    void missingTitle() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setTitle(null);
        assertViolation(entity, "title");
    }

    @Test
    @DisplayName("missing instrumentType fails validation")
    void missingInstrumentType() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setInstrumentType(null);
        assertViolation(entity, "instrumentType");
    }

    @Test
    @DisplayName("missing documentCode fails validation")
    void missingDocumentCode() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setDocumentCode(null);
        assertViolation(entity, "documentCode");
    }

    @Test
    @DisplayName("empty signers list fails validation")
    void emptySigners() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setSigners(List.of());
        assertViolation(entity, "signers");
    }

    @Test
    @DisplayName("missing recipient fails validation")
    void missingRecipient() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.setRecipient(null);
        assertViolation(entity, "recipient");
    }

    @Test
    @DisplayName("invalid signer email fails validation")
    void invalidSignerEmail() {
        EnigioInstrumentEntity entity = buildValidEntity();
        entity.getSigners().get(0).setEmail("not-an-email");
        Set<ConstraintViolation<EnigioInstrumentEntity>> violations = validator.validate(entity);
        assertThat(violations).anyMatch(v -> v.getMessage().contains("email"));
    }

    @Test
    @DisplayName("all instrument types map to Enigio values")
    void instrumentTypeMappings() {
        assertThat(InstrumentType.PROMISSORY_NOTE.toEnigioValue()).isEqualTo("Promissory note");
        assertThat(InstrumentType.BILL_OF_LADING.toEnigioValue()).isEqualTo("Bill of lading");
        assertThat(InstrumentType.LETTER_OF_CREDIT.toEnigioValue()).isEqualTo("Letter of credit");
        assertThat(InstrumentType.GUARANTEE.toEnigioValue()).isEqualTo("Guarantee");
    }

    @Test
    @DisplayName("document codes match Enigio API enum values")
    void documentCodes() {
        assertThat(DocumentCode.NEG.name()).isEqualTo("NEG");
        assertThat(DocumentCode.TTL.name()).isEqualTo("TTL");
        assertThat(DocumentCode.AGT.name()).isEqualTo("AGT");
    }

    @Test
    @DisplayName("entity tracks all 9 step results")
    void stepResultFields() {
        EnigioInstrumentEntity entity = buildValidEntity();

        // Group 1
        entity.setPdfGenerated(true);
        entity.setTraceOriginalId("to_abc123");
        entity.setVersionKey("v1_key");
        entity.setAttachmentVersionKey("v2_key");

        // Group 2
        entity.setSignersAdded(true);
        entity.setSigningEmailsSent(true);
        entity.setSigningStatus("SIGNED");

        // Group 3
        entity.setValidationResult("VALID");
        entity.setEnvelopeDraftId("draft_xyz");
        entity.setEnvelopeTraceId("to_env_sealed");
        entity.setEnvelopeVersionKey("env_v1");
        entity.setTransferId("xfr_001");

        assertThat(entity.getTraceOriginalId()).isNotNull();
        assertThat(entity.getSigningStatus()).isEqualTo("SIGNED");
        assertThat(entity.getValidationResult()).isEqualTo("VALID");
        assertThat(entity.getTransferId()).isNotNull();
    }

    // === Helpers ===

    static EnigioInstrumentEntity buildValidEntity() {
        EnigioInstrumentEntity entity = new EnigioInstrumentEntity();
        entity.setReference("INV-2026-001");
        entity.setTitle("Promissory Note — Acme / GlobalBank");
        entity.setContent("Full contract terms...");
        entity.setInstrumentType(InstrumentType.PROMISSORY_NOTE);
        entity.setDocumentCode(DocumentCode.NEG);
        entity.setParties(List.of(
                new Party("Acme Corp", "ISSUER", "556677-8899"),
                new Party("Global Bank", "BENEFICIARY", null)
        ));
        entity.setSigners(List.of(
                new Signer("Jane Doe", "jane@acme.com", "+46700000001", "CEO", "Acme Corp", 1),
                new Signer("John Smith", "john@acme.com", "+46700000002", "CFO", "Acme Corp", 2)
        ));
        entity.setRecipient(new Recipient("Global Bank Ops", "ops@globalbank.com"));
        return entity;
    }

    private void assertViolation(EnigioInstrumentEntity entity, String fieldName) {
        Set<ConstraintViolation<EnigioInstrumentEntity>> violations = validator.validate(entity);
        assertThat(violations)
                .isNotEmpty()
                .anyMatch(v -> v.getPropertyPath().toString().equals(fieldName));
    }
}
