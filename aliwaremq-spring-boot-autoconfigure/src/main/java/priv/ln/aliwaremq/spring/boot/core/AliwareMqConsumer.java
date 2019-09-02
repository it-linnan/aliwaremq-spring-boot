package priv.ln.aliwaremq.spring.boot.core;

import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * 阿里云消息队列消费者接口
 * <p>订阅单一主题时, 需实现getName</p>
 * <p>订阅多个主题时, 需实现getNames</p>
 * <p>需分别订阅标签时, 需实现getTags</p>
 *
 * @author linnan
 * @version 2019/8/27
 * @since 0.0.1
 */
public interface AliwareMqConsumer<T> {

    /**
     * 返回订阅队列自定义名称
     *
     * @return 订阅队列自定义名称
     */
    default String getName() {
        return StringUtils.EMPTY;
    }

    /**
     * 订阅队列自定义名称集合
     *
     * @return 队列自定义名称集合
     */
    default List<String> getNames() {
        return Collections.emptyList();
    }

    /**
     * 订阅标签集合
     *
     * @return 标签集合
     */
    default List<String> getTags() {
        return Collections.emptyList();
    }

    /**
     * 消费处理
     *
     * @param message 消息
     */
    void onMessage(T message);
}
