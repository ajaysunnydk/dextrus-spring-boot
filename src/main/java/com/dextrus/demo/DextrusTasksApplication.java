package com.dextrus.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import com.dextrus.demo.configuration.WebConfig;
@Import(WebConfig.class)


@SpringBootApplication
public class DextrusTasksApplication {

	public static void main(String[] args) {
		SpringApplication.run(DextrusTasksApplication.class, args);
	}

}
