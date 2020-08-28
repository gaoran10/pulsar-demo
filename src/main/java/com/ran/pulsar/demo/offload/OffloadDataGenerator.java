package com.ran.pulsar.demo.offload;

import com.ran.pulsar.demo.model.User;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.concurrent.TimeUnit;

@Slf4j
public class OffloadDataGenerator {

    private final  static String topic = "public/default/user";

    public void prepareOffloadData() throws Exception {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        @Cleanup
        Producer<User> producer = pulsarClient.newProducer(Schema.JSON(User.class))
                .topic(topic)
                .create();

        @Cleanup
        Consumer<User> consumer = pulsarClient.newConsumer(Schema.JSON(User.class))
                .topic(topic)
                .subscriptionName("test")
                .subscribe();

        long currentLedgerId = -1;
        long preLedgerId = -1;
        int index = 0;
        while (currentLedgerId <= preLedgerId) {
            User user = new User();
            index ++;
            user.setName("user-" + index);
            user.setAge(18);
            user.setSex(index % 2 == 0 ? "male" : "female");
            MessageIdImpl messageId = (MessageIdImpl) producer.newMessage().value(user).send();
            if (preLedgerId == -1) {
                preLedgerId = messageId.getLedgerId();
            }
            currentLedgerId = messageId.getLedgerId();
            Thread.sleep(1000);
            System.out.println("send message. index: " + index +
                    ", preLedgerId: " + preLedgerId +
                    ", currentLedgerId: " + currentLedgerId);
        }
        System.out.println("generate finish.");
    }

    public void consumeOffloadData() throws PulsarClientException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        @Cleanup
        Consumer<User> consumer = pulsarClient.newConsumer(Schema.JSON(User.class))
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        while (true) {
            Message<User> message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            System.out.println("receive user. name: " + message.getValue().getName());
        }
        System.out.println("consume finish.");
    }

    public void deleteOffloadedData(long ledgerId) {
        // delete the first ledger, so that we cannot possibly read from it
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers("127.0.0.1:2181");
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(ledgerId);
            System.out.println("delete finish.");
        } catch (Exception e) {
            log.error("Failed to delete from BookKeeper.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        OffloadDataGenerator offloadDataGenerator = new OffloadDataGenerator();
//        offloadDataGenerator.deleteOffloadedData(683);
        offloadDataGenerator.consumeOffloadData();
    }

    // select * from pulsar."public/default"."user" order by __publish_time__ asc;

}
