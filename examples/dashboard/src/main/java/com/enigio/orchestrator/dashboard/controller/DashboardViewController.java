package com.enigio.orchestrator.dashboard.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DashboardViewController {

    @GetMapping("/")
    public String index() {
        return "index";
    }
}
