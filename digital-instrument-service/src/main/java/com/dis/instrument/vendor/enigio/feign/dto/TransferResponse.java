package com.dis.instrument.vendor.enigio.feign.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TransferResponse(String transferId) {}
