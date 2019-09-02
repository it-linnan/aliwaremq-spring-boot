package priv.ln.aliwaremq.spring.boot.autoconfigure;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.google.gson.Gson;
import priv.ln.aliwaremq.spring.boot.config.AliwareMqProperties;
import priv.ln.aliwaremq.spring.boot.core.AliwareMqConsumer;
import priv.ln.aliwaremq.spring.boot.core.AliwareMqProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * 阿里云消息队列自动配置
 * <p>自动配置条件:</p>
 * <p>存在AliwareMqProperties和AliwareMqConsumer类型的bean</p>
 *
 * @author linnan
 * @version 2019/8/27
 * @see AliwareMqProperties
 * @see AliwareMqConsumer
 */
@Slf4j
@Configuration
@EnableConfigurationProperties
@ConditionalOnBean({AliwareMqConsumer.class})
public class AliwareMqConfiguration implements ApplicationContextAware, SmartInitializingSingleton {
    private Map<String, AliwareMqConsumer> consumerMap;
    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        consumerMap = applicationContext.getBeansOfType(AliwareMqConsumer.class);
        AliwareMqConsumerRegistry aliwareMqConsumerRegistry = applicationContext.getBean(AliwareMqConsumerRegistry.class);
        if (Objects.nonNull(consumerMap)) {
            consumerMap.forEach(aliwareMqConsumerRegistry::registerConsumer);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public Gson gson() {
        return new Gson();
    }

    @Bean
    public AliwareMqProperties aliwareMqProperties() {
        return new AliwareMqProperties();
    }

    /**
     * 注册生产者
     *
     * @param aliwareMqProperties 消息队列配置
     * @return 生产者bean
     */
    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public ProducerBean producerBean(AliwareMqProperties aliwareMqProperties) {
        if (aliwareMqProperties.getAccessKey() == null || "".equals(aliwareMqProperties.getAccessKey())) {
            aliwareMqProperties.setAccessKey(PropertyKeyConst.AccessKey);
        }
        if (aliwareMqProperties.getSecretKey() == null || "".equals(aliwareMqProperties.getSecretKey())) {
            aliwareMqProperties.setSecretKey(PropertyKeyConst.SecretKey);
        }
        ProducerBean producer = new ProducerBean();
        Properties properties = new Properties();
        setProperty(properties, PropertyKeyConst.NAMESRV_ADDR, aliwareMqProperties.getNameSrvAddr());
        setProperty(properties, PropertyKeyConst.AccessKey, aliwareMqProperties.getAccessKey());
        setProperty(properties, PropertyKeyConst.SecretKey, aliwareMqProperties.getSecretKey());
        setProperty(properties, PropertyKeyConst.GROUP_ID, aliwareMqProperties.getProducer().getGroup());
        producer.setProperties(properties);
        return producer;
    }

    @Bean
    public AliwareMqProducer aliwareMqProducer() {
        return new AliwareMqProducer();
    }

    @Bean
    public AliwareMqConsumerRegistry aliwareMqConsumerRegistry() {
        return new AliwareMqConsumerRegistry();
    }

    private void setProperty(Properties property, String key, String value) {
        if (value == null) {
            return;
        }
        property.setProperty(key, value);
    }
}