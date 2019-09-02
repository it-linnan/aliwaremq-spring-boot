package priv.ln.aliwaremq.spring.boot.core;

import com.aliyun.openservices.ons.api.*;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 阿里云消息队列生产者
 *
 * @author linnan
 * @version 2019/8/27
 * @since 0.0.1
 */
@Component
public class AliwareMqProducer {
    @Autowired
    private Producer producer;
    @Autowired
    private Gson gson;

    /**
     * 同步发送
     *
     * @param msg 消息
     * @return 发送结果
     */
    public SendResult send(Message msg) {
        return producer.send(msg);
    }

    /**
     * 同步发送
     *
     * @param topic 主题
     * @param msg   消息体
     * @param <T>   泛型
     * @return 发送结果
     */
    public <T> SendResult send(String topic, T msg) {
        Message message = convert(topic, msg);
        return producer.send(message);
    }

    /**
     * 单向发送
     *
     * @param msg 消息
     */
    public void sendOneway(Message msg) {
        producer.sendOneway(msg);
    }

    /**
     * 单向发送
     *
     * @param topic 主题
     * @param msg   消息体
     * @param <T>   泛型
     */
    public <T> void sendOneway(String topic, T msg) {
        Message message = convert(topic, msg);
        producer.sendOneway(message);
    }

    /**
     * 异步发送
     *
     * @param msg         消息
     * @param onSuccess   发送成功回调
     * @param onException 发送异常回调
     */
    public void sendAsync(Message msg, Consumer<SendResult> onSuccess, Consumer<OnExceptionContext> onException) {
        SendCallback sendCallback = buildSendCallback();
        producer.sendAsync(msg, sendCallback);
    }

    /**
     * 异步发送
     *
     * @param topic       主题
     * @param msg         消息体
     * @param onSuccess   发送成功回调
     * @param onException 发送异常回调
     * @param <T>         泛型
     */
    public <T> void sendAsync(String topic, T msg, Consumer<SendResult> onSuccess, Consumer<OnExceptionContext> onException) {
        SendCallback sendCallback = buildSendCallback();
        Message message = convert(topic, msg);
        producer.sendAsync(message, sendCallback);
    }

    /**
     * 构造回调
     *
     * @return 回调
     */
    private SendCallback buildSendCallback() {
        return new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                onSuccess(sendResult);
            }

            @Override
            public void onException(OnExceptionContext context) {
                onException(context);
            }
        };
    }

    /**
     * 消息体转换
     *
     * @param topic 主题
     * @param msg   消息体
     * @param <T>   泛型
     * @return 消息
     */
    private <T> Message convert(String topic, T msg) {
        Message message = new Message();
        message.setTopic(topic);
        String msgBody = gson.toJson(msg);
        message.setBody(msgBody.getBytes(UTF_8));
        return message;
    }
}
