package crawler.tweets.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TweetsFrontierProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetsFrontierProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String payload) {
        LOGGER.info("sending follower id='{}' to topic='tweetsfrontier'", payload);
        kafkaTemplate.send("tweetsfrontier", payload);
    }
}