package com.morgan.storm;

import com.morgan.storm.context.SpringContextHolder;
import com.morgan.storm.topology.HotProductTopology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class StormEshopApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StormEshopApplication.class, args);
        SpringContextHolder springContextHolder = new SpringContextHolder();
        springContextHolder.setApplicationContext(context);
        HotProductTopology hotProductTopology = context.getBean(HotProductTopology.class);
        hotProductTopology.runStorm(args);
    }

}
