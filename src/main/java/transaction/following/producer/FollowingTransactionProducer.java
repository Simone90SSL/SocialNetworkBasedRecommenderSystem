package transaction.following.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class FollowingTransactionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowingTransactionProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String payload) {
        LOGGER.info("sending follower id='{}' to topic='followingtransactiona'", payload);
        kafkaTemplate.send("followingtransactiona", payload);
    }
}