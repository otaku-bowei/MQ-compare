package com.compare.config;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConfig {

    private static ConnectionFactory factory;
    private static Connection connection;

    static {
        // 初始化连接工厂
        factory = new ConnectionFactory();

        // 设置连接参数
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        // 设置其他参数
        factory.setAutomaticRecoveryEnabled(true); // 自动重连
        factory.setNetworkRecoveryInterval(5000);  // 重连间隔5秒
        factory.setRequestedHeartbeat(60);         // 心跳60秒
        factory.setConnectionTimeout(30000);       // 连接超时30秒
        factory.setHandshakeTimeout(10000);        // 握手超时10秒
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public static void closeConnection() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    /**
     * 设置工厂参数
     */
    public static void configureFactory(String host, int port,
                                        String username, String password) {
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
    }
}