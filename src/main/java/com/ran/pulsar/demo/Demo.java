package com.ran.pulsar.demo;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

public class Demo {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

        String topic = "public/default/demo-test";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.newMessage().value("Hello Pulsar").send();

        Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        Message<String> message = consumer.receive();
        System.out.println("receive msg: " + message.getValue());

        consumer.close();
        producer.close();
        pulsarClient.close();
    }

}
