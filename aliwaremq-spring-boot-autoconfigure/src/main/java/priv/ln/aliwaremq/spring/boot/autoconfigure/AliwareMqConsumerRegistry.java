package priv.ln.aliwaremq.spring.boot.autoconfigure;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.bean.ConsumerBean;
import com.aliyun.openservices.ons.api.bean.Subscription;
import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import com.google.gson.Gson;
import priv.ln.aliwaremq.spring.boot.config.AliwareMqProperties;
import priv.ln.aliwaremq.spring.boot.core.AliwareMqConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import javax.annotation.PreDestroy;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * 阿里云消息队列注册中心
 *
 * @author linnan
 * @version 2019/8/27
 * @since 0.0.1
 */
@Slf4j
@Data
public class AliwareMqConsumerRegistry implements ApplicationContextAware {
    /**
     * 消费者bean集合
     */
    private Map<String, ConsumerBean> consumerBeanMap = new HashMap<>();
    private AliwareMqProperties aliwareMqProperties;
    private Gson gson;
    private ConfigurableApplicationContext applicationContext;
    private BeanDefinitionRegistry beanDefinitionRegistry;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        this.aliwareMqProperties = applicationContext.getBean(AliwareMqProperties.class);
        this.gson = applicationContext.getBean(Gson.class);
        this.beanDefinitionRegistry = (BeanDefinitionRegistry) this.applicationContext.getBeanFactory();
    }

    @PreDestroy
    private void destroy() {
        consumerBeanMap.forEach((consumerBeanName, consumerBean) -> {
            // 注销监听
            consumerBean.shutdown();
            log.info("注销消息队列消费者, 消费者beanName:{}", consumerBeanName, consumerBeanName);
        });
    }

    /**
     * 注册消费者
     *
     * @param beanName 消费者监听bean名称
     * @param consumer 消费者监听
     */
    public void registerConsumer(String beanName, AliwareMqConsumer consumer) {
        List<String> names = new ArrayList<>();
        String name = consumer.getName();
        if (StringUtils.isNotEmpty(name)) {
            names.add(name);
        } else {
            names = consumer.getNames();
        }
        if (Objects.nonNull(names)) {
            List<String> tags;
            if (consumer.getTags() == null || consumer.getTags().isEmpty()) {
                tags = Arrays.asList("*");
            } else {
                tags = consumer.getTags();
            }
            names.forEach(consumerName -> tags.forEach(tag -> {
                String consumerBeanName = String.format("%s:%s@%s", AliwareMqConsumer.class.getName(), consumerName, tag);
                BeanDefinition beanDefinition = buildBeanDefinition(consumerName, tag, consumer);
                // 注册bean定义
                beanDefinitionRegistry.registerBeanDefinition(consumerBeanName, beanDefinition);
                ConsumerBean consumerBean = applicationContext.getBean(consumerBeanName, ConsumerBean.class);
                // 启动监听
                consumerBean.start();
                // 保存
                consumerBeanMap.put(consumerBeanName, consumerBean);
                log.info("注册消息队列消费者, 消息监听beanName:{}, 消费者beanName:{}", beanName, consumerBeanName);
            }));
        }
    }

    /**
     * 构建bean定义
     *
     * @param consumerName 消费者自定义名称
     * @param tag          标签
     * @param consumer     消费者监听
     * @return bean定义
     */
    private AbstractBeanDefinition buildBeanDefinition(String consumerName, String tag, AliwareMqConsumer consumer) {
        AliwareMqProperties.Consumer consumerProperties = aliwareMqProperties.getConsumer().get(consumerName);
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder
                .genericBeanDefinition(ConsumerBean.class, () -> buildConsumerBean(consumer, consumerProperties, tag))
                .addPropertyValue("properties", buildProperties(consumerProperties))
                .addPropertyValue("subscriptionTable", buildSubscriptionTable(consumer, consumerProperties, tag));
        AbstractBeanDefinition beanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
        return beanDefinition;
    }

    /**
     * 创建阿里云消息队列消费者bean
     *
     * @param consumer           消费者监听
     * @param consumerProperties 消费者属性
     * @param tag                标签
     * @return 阿里云消息队列消费者bean
     */
    private ConsumerBean buildConsumerBean(AliwareMqConsumer consumer, AliwareMqProperties.Consumer consumerProperties, String tag) {
        ConsumerBean consumerBean = new ConsumerBean();
        // 构造配置
        Properties properties = buildProperties(consumerProperties);
        consumerBean.setProperties(properties);
        Map<Subscription, MessageListener> subscriptionTable = buildSubscriptionTable(consumer, consumerProperties, tag);
        // 注册订阅表
        consumerBean.setSubscriptionTable(subscriptionTable);
        return consumerBean;
    }

    /**
     * 构造订阅表
     *
     * @param consumer           消费者监听
     * @param consumerProperties 消费者属性
     * @param tag                标签
     * @return 订阅表
     */
    private Map<Subscription, MessageListener> buildSubscriptionTable(AliwareMqConsumer consumer, AliwareMqProperties.Consumer consumerProperties, String tag) {
        // 订阅关系
        Map<Subscription, MessageListener> subscriptionTable = new HashMap<>();
        Subscription subscription = new Subscription();
        subscription.setTopic(consumerProperties.getTopic());
        subscription.setExpression(tag);
        // 获取消费者接收的消息类型
        Class messageType = getMessageType(consumer);
        // 判断是否是mq消息原始类型
        boolean isOriginMsg = Message.class.equals(messageType);
        subscriptionTable.put(subscription, (message, context) -> {
            try {
                if (isOriginMsg) {
                    // 消费者接受原始类型, 不转换
                    consumer.onMessage(message);
                } else {
                    // 转换为消费者接受的原始类型
                    Object msgBody = convert(message, messageType);
                    consumer.onMessage(msgBody);
                }
                return Action.CommitMessage;
            } catch (Exception e) {
                log.error("消费[主题:{},标签:{},消息id:{}]消息发生异常", message.getTopic(), message.getTag(), message.getMsgID(), e);
                return Action.ReconsumeLater;
            }
        });
        return subscriptionTable;
    }

    /**
     * 构造消费者配置
     *
     * @param consumer 消费者配置
     * @return 消费者配置
     */
    private Properties buildProperties(AliwareMqProperties.Consumer consumer) {
        Properties properties = new Properties();
        setProperty(properties, PropertyKeyConst.NAMESRV_ADDR, aliwareMqProperties.getNameSrvAddr());
        setProperty(properties, PropertyKeyConst.AccessKey, aliwareMqProperties.getAccessKey());
        setProperty(properties, PropertyKeyConst.SecretKey, aliwareMqProperties.getSecretKey());
        setProperty(properties, PropertyKeyConst.GROUP_ID, consumer.getGroup());
        setProperty(properties, PropertyKeyConst.MessageModel, consumer.getMessageModel().getModeCN());
        setProperty(properties, PropertyKeyConst.ConsumeThreadNums, String.valueOf(consumer.getConsumeThreadNums()));
        setProperty(properties, PropertyKeyConst.MaxReconsumeTimes, String.valueOf(consumer.getMaxReconsumeTimes()));
        setProperty(properties, PropertyKeyConst.ConsumeTimeout, String.valueOf(consumer.getConsumeTimeout()));
        setProperty(properties, PropertyKeyConst.SuspendTimeMillis, String.valueOf(consumer.getSuspendTimeMillis()));
        setProperty(properties, PropertyKeyConst.MaxCachedMessageAmount, String.valueOf(consumer.getMaxCachedMessageAmount()));
        setProperty(properties, PropertyKeyConst.MaxCachedMessageSizeInMiB, String.valueOf(consumer.getMaxCachedMessageSizeInMiB()));
        return properties;
    }

    /**
     * 消息体类型转换
     *
     * @param message 消息
     * @param clazz   类型
     * @return 消息体
     */
    private Object convert(Message message, Class clazz) {
        String body = new String(message.getBody());
        return gson.fromJson(body, clazz);
    }

    /**
     * 获取消费者监听程序接收的消息类型
     *
     * @param consumer 消费者处理程序
     * @param <T>      消息类型泛型
     * @return 消息类型
     */
    private <T> Class getMessageType(AliwareMqConsumer<T> consumer) {
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(consumer);
        Type[] interfaces = targetClass.getGenericInterfaces();
        Class<?> superclass = targetClass.getSuperclass();
        while ((Objects.isNull(interfaces) || 0 == interfaces.length) && Objects.nonNull(superclass)) {
            interfaces = superclass.getGenericInterfaces();
            superclass = targetClass.getSuperclass();
        }
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), AliwareMqConsumer.class)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        if (Objects.nonNull(actualTypeArguments) && actualTypeArguments.length > 0) {
                            return (Class) actualTypeArguments[0];
                        } else {
                            return Object.class;
                        }
                    }
                }
            }
            return Object.class;
        } else {
            return Object.class;
        }
    }

    private void setProperty(Properties property, String key, String value) {
        if (value == null) {
            return;
        }
        property.setProperty(key, value);
    }
}
