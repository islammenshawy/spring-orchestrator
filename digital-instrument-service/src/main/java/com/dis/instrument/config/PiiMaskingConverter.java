package com.dis.instrument.config;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.regex.Pattern;

/**
 * Logback converter that masks PII in log messages before output.
 *
 * Masks:
 * - Email addresses: alice@example.com → a***@example.com
 * - Phone numbers: +46700000001 → +46*****0001
 * - Swedish org numbers: 123456-7890 → ******-7890
 *
 * Usage in logback-spring.xml:
 *   <conversionRule conversionWord="maskedMsg" converterClass="com.dis.instrument.config.PiiMaskingConverter"/>
 *   <pattern>%d{HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %maskedMsg%n</pattern>
 */
public class PiiMaskingConverter extends ClassicConverter {

    // email: capture local part (first char) + domain
    private static final Pattern EMAIL = Pattern.compile(
            "\\b([a-zA-Z0-9])[a-zA-Z0-9._%+-]*@([a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})\\b");

    // phone: +CC followed by digits, mask middle digits keeping last 4
    private static final Pattern PHONE = Pattern.compile(
            "(\\+\\d{1,3})\\d{3,}(\\d{4})\\b");

    // Swedish org number: 6 digits, dash, 4 digits
    private static final Pattern ORG_NUMBER = Pattern.compile(
            "\\b(\\d{6})-(\\d{4})\\b");

    @Override
    public String convert(ILoggingEvent event) {
        String msg = event.getFormattedMessage();
        if (msg == null) return "";

        msg = EMAIL.matcher(msg).replaceAll("$1***@$2");
        msg = PHONE.matcher(msg).replaceAll("$1*****$2");
        msg = ORG_NUMBER.matcher(msg).replaceAll("******-$2");

        return msg;
    }
}
