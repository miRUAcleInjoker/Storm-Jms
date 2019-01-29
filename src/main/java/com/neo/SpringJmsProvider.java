package com.neo;

import org.apache.storm.jms.JmsProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

/**
 * @author wf
 * @Description SpringJmsProvider
 * @Date 2019/1/29 15:09
 */
public class SpringJmsProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;
    private Destination destination;

    /**
     * Constructs a <code>SpringJmsProvider</code> object given the name of a
     * classpath resource (the spring application context file), and the bean
     * names of a JMS connection factory and destination.
     *
     * @param appContextClasspathResource - the spring configuration file (classpath resource)
     * @param connectionFactoryBean       - the JMS connection factory bean name
     * @param destinationBean             - the JMS destination bean name
     */
    public SpringJmsProvider(String appContextClasspathResource, String connectionFactoryBean, String destinationBean) {
        ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
        this.connectionFactory = (ConnectionFactory) context.getBean(connectionFactoryBean);
        this.destination = (Destination) context.getBean(destinationBean);
    }

    public ConnectionFactory connectionFactory() {
        return this.connectionFactory;
    }

    public Destination destination() {
        return this.destination;
    }
}
