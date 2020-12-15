package com.morgan.storm;

import com.morgan.storm.context.SpringContextHolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @Description
 * @Author Morgan
 * @Date 2020/12/14 16:06
 **/
@SpringBootApplication
public class SpringBootApp {

    public static void run(String args) {
        ConfigurableApplicationContext context =  SpringApplication.run(SpringApplication.class,args);
        SpringContextHolder springContextHolder = new SpringContextHolder();
        springContextHolder.setApplicationContext(context);
    }
}
