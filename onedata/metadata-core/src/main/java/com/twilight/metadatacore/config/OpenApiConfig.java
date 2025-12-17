package com.twilight.metadatacore.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI metadataOpenAPI() {
        return new OpenAPI()
                .components(new Components())
                .info(new Info()
                        .title("Metadata Core API")
                        .description("Unified metadata entity discovery and lineage APIs")
                        .version("v1")
                        .contact(new Contact().name("Metadata Core").url("https://example.com")));
    }
}
