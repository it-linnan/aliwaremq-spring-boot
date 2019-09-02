package priv.ln.aliwaremq.spring.boot.config;

import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * 阿里云消息队列配置参数
 *
 * @author linnan
 * @version 2019/8/27
 */
@Data
@ConfigurationProperties("aliwaremq")
public class AliwareMqProperties {
    /**
     * name server地址
     */
    private String nameSrvAddr;
    /**
     * 阿里云accessKey
     */
    private String accessKey;
    /**
     * 阿里云secretKey
     */
    private String secretKey;
    /**
     * 生产者配置
     */
    private Producer producer;
    /**
     * 消费者配置
     */
    private Map<String, Consumer> consumer;

    @Data
    public static class Producer {
        /**
         * 生产者组
         */
        private String group;
        /**
         * 消息发送的超时时间, 单位:毫秒, 默认值3000
         */
        private int sendMsgTimeoutMillis = 3000;
    }

    @Data
    public static class Consumer {
        /**
         * 消费者组
         */
        private String group;
        /**
         * 消费者订阅主题
         */
        private String topic;
        /**
         * 消费模式, CLUSTERING集群消费, BROADCASTING广播消费, 默认值CLUSTERING
         */
        private MessageModel messageModel = MessageModel.CLUSTERING;
        /**
         * 消费线程数, 默认值20
         */
        private int consumeThreadNums = 20;
        /**
         * 消息消费失败的最大重试次数, 默认值16
         */
        private int maxReconsumeTimes = 16;
        /**
         * 每条消息消费的最大超时时间, 单位:分钟, 默认值15
         */
        private int consumeTimeout = 15;
        /**
         * 只适用于顺序消息, 消息消费失败的重试间隔时间
         */
        private int suspendTimeMillis;
        /**
         * 最大缓存消息数据, 单位:条, 默认值1000
         */
        private int maxCachedMessageAmount;
        /**
         * 最大缓存消息大小, 取值范围16 MB ~ 2 GB, 默认值512 MB
         */
        private int maxCachedMessageSizeInMiB = 512;
    }
}
