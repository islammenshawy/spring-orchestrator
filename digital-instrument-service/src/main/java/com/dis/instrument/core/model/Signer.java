package com.dis.instrument.core.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A required signatory on a trade instrument.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Signer {

    @NotBlank(message = "signer name is required")
    private String name;

    @NotBlank(message = "signer email is required")
    @Email(message = "invalid email format")
    private String email;

    @NotBlank(message = "signer phone is required")
    private String phone;

    @NotBlank(message = "capacity is required")
    private String capacity; // CEO, CFO, Authorized Signatory

    private String organisation;

    /** Signing order — signer 1 signs first, then signer 2 receives their link */
    private int order;
}
