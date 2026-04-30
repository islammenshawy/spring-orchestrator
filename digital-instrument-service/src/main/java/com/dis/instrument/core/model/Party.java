package com.dis.instrument.core.model;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A party involved in the trade instrument (issuer, beneficiary, etc.).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Party {

    @NotBlank(message = "party name is required")
    private String name;

    @NotBlank(message = "party role is required")
    private String role; // ISSUER, BENEFICIARY, GUARANTOR, CARRIER

    private String orgNumber;
}
