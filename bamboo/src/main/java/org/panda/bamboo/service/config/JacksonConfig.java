package org.panda.bamboo.service.config;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2019-08-30 10:21
 */
@Configuration
public class JacksonConfig {
  @Bean
  public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
    ObjectMapper objectMapper = builder.createXmlMapper(false).build();
    objectMapper.registerModule(new DefaultScalaModule());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    SimpleModule module = new SimpleModule();
    objectMapper.registerModule(module);
    return objectMapper;
  }
}
