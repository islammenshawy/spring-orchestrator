package com.dis.instrument.core.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The recipient who will receive possession of the sealed envelope.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Recipient {

    @NotBlank(message = "recipient name is required")
    private String name;

    @NotBlank(message = "recipient email is required")
    @Email(message = "invalid email format")
    private String email;
}
