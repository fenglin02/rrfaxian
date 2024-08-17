package com.rrfx;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy(exposeProxy = true)
@MapperScan("com.rrfx.mapper")
@SpringBootApplication
public class RrFaxianApplication {

    public static void main(String[] args) {
        SpringApplication.run(RrFaxianApplication.class, args);
    }

}
