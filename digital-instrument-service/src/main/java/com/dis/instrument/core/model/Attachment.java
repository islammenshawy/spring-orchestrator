package com.dis.instrument.core.model;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A supporting document to attach to the instrument.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Attachment {

    @NotBlank(message = "filename is required")
    private String filename;

    @NotBlank(message = "data is required")
    private String data; // base64 content

    private String comment;
}
