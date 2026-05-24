package com.docuseal.signing.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EmailService {

    private final JavaMailSender mailSender;
    private final boolean enabled;

    public EmailService(JavaMailSender mailSender) {
        this.mailSender = mailSender;
        // Check if mail is actually configured
        this.enabled = mailSender != null;
    }

    public void sendPartyBNotification(String to, String partyAName, String reference) {
        String subject = "Your turn to sign: " + reference;
        String body = String.format(
                "%s has signed the document '%s'. It's now your turn to review and sign.\n\n"
                + "Please check your email for the DocuSeal signing link.\n\n"
                + "Thank you.",
                partyAName, reference);
        send(to, subject, body);
    }

    public void sendCompletionNotification(String to, String reference, String documentUrl) {
        String subject = "Document fully signed: " + reference;
        String body = String.format(
                "The document '%s' has been signed by all parties.\n\n"
                + "Signed document: %s\n\n"
                + "Thank you.",
                reference, documentUrl != null ? documentUrl : "(available in DocuSeal)");
        send(to, subject, body);
    }

    private void send(String to, String subject, String body) {
        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setTo(to);
            message.setSubject(subject);
            message.setText(body);
            message.setFrom("noreply@docuseal-test.com");
            mailSender.send(message);
            log.info("[Email] Sent '{}' to {}", subject, to);
        } catch (Exception e) {
            // Don't fail the flow if email fails — log and continue
            log.warn("[Email] Failed to send '{}' to {}: {}. Continuing without email.", subject, to, e.getMessage());
        }
    }
}
