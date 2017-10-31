package crawler.following.frontier.producer;

        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import org.springframework.beans.factory.annotation.Autowired;
        import org.springframework.kafka.core.KafkaTemplate;
        import org.springframework.stereotype.Component;

@Component
public class FollowingFrontierProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowingFrontierProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String payload) {
        LOGGER.debug("sending follower id='{}' to topic='followingfrontier'", payload);
        kafkaTemplate.send("followingfrontier", payload);
    }
}