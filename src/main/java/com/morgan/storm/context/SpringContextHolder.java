package com.morgan.storm.context;

import org.apache.commons.lang3.Validate;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @Description 获取Spring Bean实例的工具类
 * @Author Morgan
 * @Date 2020/12/14 15:56
 **/
@Component
public class SpringContextHolder implements ApplicationContextAware, DisposableBean {

    private static ApplicationContext context;

    public static <T> T getBean(String name){
        assertContextInjected();
        return (T) context.getBean(name);
    }

    public static <T> T getBean(Class<T> requiredType){
        assertContextInjected();
        return context.getBean(requiredType);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextHolder.context = applicationContext;
    }

    /**
     * 清除SpringContextHolder中的ApplicationContext为Null.
     */
    public static void clearHolder() {
        System.out.println("清除SpringContextHolder中的ApplicationContext:" + context);
        context = null;
    }

    @Override
    public void destroy() throws Exception {
        SpringContextHolder.clearHolder();
    }

    /**
     * 检查ApplicationContext不为空.
     */
    private static void assertContextInjected() {
        Validate.validState(context != null, "applicaitonContext属性未注入, 请先定义SpringContextHolder.");
    }
}
