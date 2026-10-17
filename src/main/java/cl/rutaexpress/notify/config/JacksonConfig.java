package cl.rutaexpress.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    // La autoconfiguración de Jackson necesita spring-web (Jackson2ObjectMapperBuilder), que este
    // servicio no tiene porque no expone HTTP; por eso se declara explícitamente.
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
