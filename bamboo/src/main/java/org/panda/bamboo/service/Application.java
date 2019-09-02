package org.panda.bamboo.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2019-08-30 10:14
 */
@SpringBootApplication
@EnableAutoConfiguration
@EnableSwagger2
@ComponentScan({
    "org.panda.bamboo.service"
})
public class Application {
  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
}
