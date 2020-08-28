package com.ran.pulsar.demo.pulsarsql;

import com.ran.pulsar.demo.model.Product;
import lombok.Cleanup;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProductDataGenerator {

    private final static String XPHONE_TOPIC = "persistent://public/default/xphone-info-topic";
    private PulsarClient pulsarClient;

    public void prepareData() throws PulsarClientException {
        pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        Consumer<Product> consumer = pulsarClient.newConsumer(Schema.JSON(Product.class))
                .topic(XPHONE_TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        consumer.close();

        @Cleanup
        Producer<Product> producer = pulsarClient.newProducer(Schema.JSON(Product.class))
                .topic(XPHONE_TOPIC)
                .batchingMaxMessages(2000) // default 1000
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS) // default 1 ms
                .compressionType(CompressionType.ZSTD)
                .create();

        for (Product product : getXPhoneDataList()) {
            producer.newMessage().value(product).sendAsync();
        }

        producer.flush();

        producer.newMessage().value(new Product()).send();

        pulsarClient.close();
        System.out.println("send finish");
    }

    public List<Product> getXPhoneDataList() {
        List<Product> products = new ArrayList<>();
        Date today = new Date();
        for (int i = 100; i > 0; i--) {
            Product product = new Product();
            product.setName("XPhone");
            Date date = DateUtils.addDays(today, -i);
            product.setUpdateTime(date.getTime());
            product.setUpdateTimeStr(getDateStr(date));
            if (i <= 100 && i > 80) {
                product.setPrice(5999.0);
            } else if (i <= 80 && i > 50) {
                product.setPrice(5599.0);
            } else if (i <= 50 && i > 40) {
                product.setPrice(5200.0);
            } else if (i <= 40 && i > 30) {
                product.setPrice(5599.0);
            } else if (i <= 30) {
                product.setPrice(5899.0);
            }
            products.add(product);
        }
        return products;
    }

    private String getDateStr(Date date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(date);
    }

    public static void main(String[] args) throws PulsarClientException {
        ProductDataGenerator generator = new ProductDataGenerator();
        generator.prepareData();
    }

}
