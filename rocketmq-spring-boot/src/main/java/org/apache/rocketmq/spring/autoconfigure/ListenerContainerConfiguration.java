/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

// @Configuration 注解，本身自带 @Component 注解。
@Configuration
public class ListenerContainerConfiguration implements ApplicationContextAware, SmartInitializingSingleton {
    private final static Logger log = LoggerFactory.getLogger(ListenerContainerConfiguration.class);

    private ConfigurableApplicationContext applicationContext;
	/**
	 * 计数器，用于在 {@link #registerContainer(String, Object)} 方法中，创建 DefaultRocketMQListenerContainer Bean 时，生成 Bean 的名字。
	 */
    private AtomicLong counter = new AtomicLong(0);

    private StandardEnvironment environment;

    private RocketMQProperties rocketMQProperties;

    private ObjectMapper objectMapper;

    public ListenerContainerConfiguration(ObjectMapper rocketMQMessageObjectMapper,
        StandardEnvironment environment,
        RocketMQProperties rocketMQProperties) {
        this.objectMapper = rocketMQMessageObjectMapper;
        this.environment = environment;
        this.rocketMQProperties = rocketMQProperties;
    }

	// 实现自 ApplicationContextAware 接口
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

	// 实现自 SmartInitializingSingleton 接口
    @Override
    public void afterSingletonsInstantiated() {
    	//获取 @RocketMQMessageListener 注解的所有Bean
        Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQMessageListener.class);

        if (Objects.nonNull(beans)) {
            beans.forEach(this::registerContainer);
        }
    }

    // 生成（注册）对应的 DefaultRocketMQListenerContainer Bean 对象
    private void registerContainer(String beanName, Object bean) {
    	// 获得 Bean 对应的 Class 类名。因为有可能被 AOP 代理过
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        // 如果未实现 RocketMQListener 接口，直接抛出 IllegalStateException 异常
        if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
        }

        // 获得 @RocketMQMessageListener 注解
        RocketMQMessageListener annotation = clazz.getAnnotation(RocketMQMessageListener.class);
        // 校验注解配置
        validate(annotation);

        // 生成 DefaultRocketMQListenerContainer Bean 的名字
        String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(),
            counter.incrementAndGet());
        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;
		// 创建 DefaultRocketMQListenerContainer Bean 对象，并注册到 Spring 容器中。
        genericApplicationContext.registerBean(containerBeanName, DefaultRocketMQListenerContainer.class,
            () -> createRocketMQListenerContainer(bean, annotation));
        // 从 Spring 容器中，获得刚注册的 DefaultRocketMQListenerContainer Bean 对象
        DefaultRocketMQListenerContainer container = genericApplicationContext.getBean(containerBeanName,
            DefaultRocketMQListenerContainer.class);
        // 如果未启动，则进行启动
        if (!container.isRunning()) {
            try {
                container.start();
            } catch (Exception e) {
                log.error("Started container failed. {}", container, e);
                throw new RuntimeException(e);
            }
        }

        log.info("Register the listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }

	/**
	 * 给每个带有注解的 @RocketMQMessageListener Bean 对象，生成对应的 DefaultRocketMQListenerContainer Bean 对象。
	 * DefaultRocketMQListenerContainer 类，正如其名，是 DefaultRocketMQListener（RocketMQ 消费者的监听器）容器，
	 * 负责创建 DefaultRocketMQListener 对象，并启动其对应的 DefaultMQPushConsumer（消费者），从而消费消息。
	 */
    private DefaultRocketMQListenerContainer createRocketMQListenerContainer(Object bean, RocketMQMessageListener annotation) {
        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();

        container.setNameServer(rocketMQProperties.getNameServer());
        container.setTopic(environment.resolvePlaceholders(annotation.topic()));
        container.setConsumerGroup(environment.resolvePlaceholders(annotation.consumerGroup()));
        container.setRocketMQMessageListener(annotation);
        container.setRocketMQListener((RocketMQListener) bean);
        container.setObjectMapper(objectMapper);

        return container;
    }

    private void validate(RocketMQMessageListener annotation) {
    	// 禁止顺序消费 + 广播消费
        if (annotation.consumeMode() == ConsumeMode.ORDERLY &&
            annotation.messageModel() == MessageModel.BROADCASTING) {
            throw new BeanDefinitionValidationException(
                "Bad annotation definition in @RocketMQMessageListener, messageModel BROADCASTING does not support ORDERLY message!");
        }
    }
}
