package com.dis.instrument.config;

import com.dis.instrument.model.*;

import com.dis.instrument.model.*;
import com.dis.instrument.flow.EnigioInstrumentEntity;
import com.orchestrator.starter.domain.FlowStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Seeds the database with sample completed flows for demo/testing.
 * Only runs if the collection is empty (first boot).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@org.springframework.context.annotation.Profile("!prod")
public class SeedDataInitializer implements CommandLineRunner {

    private final MongoTemplate mongoTemplate;

    @Override
    public void run(String... args) {
        long count = mongoTemplate.getCollection("dis_instrument_flows").countDocuments();
        if (count > 0) {
            log.info("[Seed] {} existing flows found, skipping seed data", count);
            return;
        }

        log.info("[Seed] No flows found, seeding sample data...");

        // 1. Completed promissory note
        seed(buildFlow(
                "PN-2026-00001", "Promissory Note — Acme Corp to Global Bank",
                "Acme Corp hereby promises to pay Global Bank the sum of EUR 1,000,000 on or before January 1, 2027, with interest at the rate of 3.5% per annum.",
                InstrumentType.PROMISSORY_NOTE, DocumentCode.NEG,
                List.of(new Party("Acme Corp", "ISSUER", "556677-8899"), new Party("Global Bank", "BENEFICIARY", null)),
                List.of(
                        new Signer("Jane Doe", "jane.doe@acme-corp.com", "+46701234567", "CEO", "Acme Corp", 1),
                        new Signer("John Smith", "john.smith@acme-corp.com", "+46709876543", "CFO", "Acme Corp", 2)
                ),
                new Recipient("Global Bank Operations", "ops@globalbank.com"),
                FlowStatus.COMPLETED,
                Map.of("placeOfIssue", "Stockholm", "currencyCode", "EUR", "amountInFigures", "1,000,000")
        ));

        // 2. Completed bill of lading
        seed(buildFlow(
                "BL-2026-00042", "Bill of Lading — Nordic Shipping to Rotterdam Port",
                "Shipment of 500 containers of electronic components from Stockholm to Rotterdam. Vessel: MV Nordic Star. Voyage: NS-2026-07.",
                InstrumentType.BILL_OF_LADING, DocumentCode.TTL,
                List.of(new Party("Nordic Shipping AB", "CARRIER", "559988-1122"), new Party("Rotterdam Port Authority", "CONSIGNEE", null)),
                List.of(new Signer("Erik Larsson", "erik@nordic-shipping.se", "+46708001122", "Operations Director", "Nordic Shipping AB", 1)),
                new Recipient("Rotterdam Port Authority", "docs@portofrotterdam.nl"),
                FlowStatus.COMPLETED,
                Map.of("vesselName", "MV Nordic Star", "voyage", "NS-2026-07", "containers", "500")
        ));

        // 3. In-progress guarantee (stuck at signing)
        var guarantee = buildFlow(
                "GR-2026-00017", "Bank Guarantee — Trade Finance Ltd",
                "Trade Finance Ltd guarantees payment of USD 250,000 to Supplier Corp for goods delivered under purchase order PO-2026-3344.",
                InstrumentType.GUARANTEE, DocumentCode.NEG,
                List.of(new Party("Trade Finance Ltd", "GUARANTOR", "112233-4455"), new Party("Supplier Corp", "BENEFICIARY", null)),
                List.of(
                        new Signer("Maria Garcia", "maria@tradefinance.com", "+34600112233", "Managing Director", "Trade Finance Ltd", 1),
                        new Signer("Carlos Rodriguez", "carlos@tradefinance.com", "+34600445566", "Head of Risk", "Trade Finance Ltd", 2)
                ),
                new Recipient("Supplier Corp Legal", "legal@suppliercorp.com"),
                FlowStatus.IN_PROGRESS,
                Map.of("guaranteeAmount", "250,000 USD", "purchaseOrder", "PO-2026-3344")
        );
        guarantee.setCurrentStep(FlowStep.AWAIT_SIGNATURES.name());
        guarantee.setTraceOriginalId("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6abcd");
        guarantee.setVersionKey("v1_seed_guarantee");
        guarantee.setAttachmentVersionKey("NONE");
        guarantee.setPdfGenerated(true);
        guarantee.setPreparationNotified(true);
        guarantee.setSigningApproved(true);
        guarantee.setSignersAdded(true);
        guarantee.setSigningEmailsSent(true);
        guarantee.setWebhookRegistered(true);
        guarantee.setSignaturesRequired(2);
        guarantee.setSignaturesReceived(1);
        guarantee.setSigningStatus(SigningStatus.PARTIALLY_SIGNED);
        guarantee.setSigningStartedAt(Instant.now().minusSeconds(3600));
        seed(guarantee);

        // 4. Failed invoice (signing rejected)
        var invoice = buildFlow(
                "INV-2026-00088", "Invoice — Component Supply Agreement",
                "Invoice for 10,000 units of Component A at EUR 45.00 per unit. Total: EUR 450,000.",
                InstrumentType.INVOICE, DocumentCode.AGT,
                List.of(new Party("MicroTech GmbH", "SELLER", "DE-HRB-123456")),
                List.of(new Signer("Hans Mueller", "hans@microtech.de", "+49170112233", "CEO", "MicroTech GmbH", 1)),
                new Recipient("BuyerCo Procurement", "procurement@buyerco.com"),
                FlowStatus.FAILED,
                null
        );
        invoice.setCurrentStep(FlowStep.AWAIT_SIGNATURES.name());
        invoice.setTraceOriginalId("f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6f1e2d3c4b5a6wxyz");
        invoice.setVersionKey("v1_seed_invoice");
        invoice.setPdfGenerated(true);
        invoice.setPreparationNotified(true);
        invoice.setSigningApproved(true);
        invoice.setSignersAdded(true);
        invoice.setSigningEmailsSent(true);
        invoice.setSigningStatus(SigningStatus.REJECTED);
        invoice.setErrorMessage("Signature rejected by signer: Hans Mueller — document terms disputed");
        seed(invoice);

        log.info("[Seed] Seeded 4 sample flows (2 completed, 1 in-progress, 1 failed)");
    }

    private EnigioInstrumentEntity buildFlow(String reference, String title, String content,
                                              InstrumentType type, DocumentCode code,
                                              List<Party> parties, List<Signer> signers,
                                              Recipient recipient, FlowStatus status,
                                              Map<String, Object> customData) {
        var flow = new EnigioInstrumentEntity();
        flow.setReference(reference);
        flow.setTitle(title);
        flow.setContent(content);
        flow.setInstrumentType(type);
        flow.setDocumentCode(code);
        flow.setParties(parties);
        flow.setSigners(signers);
        flow.setRecipient(recipient);
        flow.setCustomData(customData);
        flow.setFlowType("enigio-instrument");
        flow.setCorrelationId(java.util.UUID.randomUUID().toString());
        flow.setStatus(status);
        flow.setCreatedAt(Instant.now().minusSeconds(86400 + (long)(Math.random() * 86400)));
        flow.setUpdatedAt(Instant.now().minusSeconds((long)(Math.random() * 3600)));

        if (status == FlowStatus.COMPLETED) {
            flow.setCurrentStep(FlowStep.TRANSFER_DOCUMENT.name());
            flow.setPdfGenerated(true);
            flow.setTraceOriginalId(java.util.UUID.randomUUID().toString().replace("-", "") +
                    java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 32));
            flow.setVersionKey("v3_" + java.util.UUID.randomUUID().toString().substring(0, 8));
            flow.setAttachmentVersionKey("v2_" + java.util.UUID.randomUUID().toString().substring(0, 8));
            flow.setPreparationNotified(true);
            flow.setPreparationNotifiedAt(flow.getCreatedAt().plusSeconds(30));
            flow.setSigningApproved(true);
            flow.setSignersAdded(true);
            flow.setSigningEmailsSent(true);
            flow.setWebhookRegistered(true);
            flow.setSignaturesRequired(signers.size());
            flow.setSignaturesReceived(signers.size());
            flow.setSigningStatus(SigningStatus.SIGNED);
            flow.setSigningStartedAt(flow.getCreatedAt().plusSeconds(60));
            flow.setSigningNotified(true);
            flow.setSigningNotifiedAt(flow.getCreatedAt().plusSeconds(300));
            flow.setDeliveryApproved(true);
            flow.setValidationResult("VALID");
            flow.setEnvelopeDraftId("draft_seed_" + reference.toLowerCase().replace("-", ""));
            flow.setEnvelopeTraceId(java.util.UUID.randomUUID().toString().replace("-", "") +
                    java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 32));
            flow.setEnvelopeVersionKey("v1_env_" + java.util.UUID.randomUUID().toString().substring(0, 8));
            flow.setTransferId(java.util.UUID.randomUUID().toString());
        }

        return flow;
    }

    private void seed(EnigioInstrumentEntity flow) {
        mongoTemplate.save(flow, "dis_instrument_flows");
        log.info("[Seed] {} — {} ({})", flow.getReference(), flow.getInstrumentType(), flow.getStatus());
    }
}
