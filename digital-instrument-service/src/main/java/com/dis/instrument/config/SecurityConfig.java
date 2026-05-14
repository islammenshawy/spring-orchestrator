package com.dis.instrument.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * API key authentication for DIS endpoints.
 *
 * Exemptions:
 * - /actuator/** — health probes (Kubernetes)
 * - /webhooks/** — vendor callbacks (protected by network policy + DB validation)
 * - /v3/api-docs/**, /swagger-ui/** — API documentation
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Value("${dis.auth.api-key}")
    private String apiKey;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/**").permitAll()
                        .requestMatchers("/webhooks/**").permitAll()
                        .requestMatchers("/v3/api-docs/**", "/swagger-ui/**").permitAll()
                        .anyRequest().authenticated()
                )
                .addFilterBefore(new ApiKeyFilter(apiKey), UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }

    /**
     * Simple API key filter — checks X-API-Key header against configured secret.
     * Returns 401 if missing or invalid.
     */
    static class ApiKeyFilter extends OncePerRequestFilter {

        private final String apiKey;

        ApiKeyFilter(String apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                        FilterChain filterChain) throws ServletException, IOException {
            String key = request.getHeader("X-API-Key");
            if (apiKey.equals(key)) {
                // Set authentication so Spring Security authorizeHttpRequests passes
                var auth = new org.springframework.security.authentication.UsernamePasswordAuthenticationToken(
                        "api-client", null, java.util.List.of());
                org.springframework.security.core.context.SecurityContextHolder.getContext().setAuthentication(auth);
            }
            filterChain.doFilter(request, response);
        }
    }
}
