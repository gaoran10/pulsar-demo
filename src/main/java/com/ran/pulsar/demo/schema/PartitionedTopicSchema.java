package com.ran.pulsar.demo.schema;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;

public class PartitionedTopicSchema {

    private final static String PARTITIONED_TOPIC = "public/default/partitioned-schema-topic";

    private PulsarClient pulsarClient;

    private void init() throws PulsarClientException {
        pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
    }

    public void beforeProduce() throws PulsarClientException {
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(PARTITIONED_TOPIC)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        consumer.close();
    }

    public void produceData() throws PulsarClientException {
        @Cleanup
        Producer<User> producer = pulsarClient.newProducer(Schema.JSON(User.class))
                .topic(PARTITIONED_TOPIC)
                .enableBatching(false)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();

        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.setName("user-" + i);
            user.setAge(18);
            user.setSex(i % 2 == 0 ? "male" : "female");
            producer.newMessage().value(user).send();
        }
        System.out.println("produce messages finish");
    }

    public void checkPartitionedTopic() throws Exception {
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(TopicName.get(PARTITIONED_TOPIC).getPartition(1).toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        Message<GenericRecord> message = consumer.receive();
        System.out.println("message " + message.getValue());
        System.out.println("receive finish.");
    }

    public void clean() throws PulsarClientException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }

    public static void main(String[] args) throws Exception {
        PartitionedTopicSchema schema = new PartitionedTopicSchema();
        schema.init();
        schema.beforeProduce();
        schema.produceData();
        schema.checkPartitionedTopic();
        schema.clean();
    }

}
