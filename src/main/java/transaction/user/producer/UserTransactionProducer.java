package transaction.user.producer;

import crawler.Crawler;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;

@Component
public class UserTransactionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserTransactionProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    public void send(CrawledUser crawledUser) {
        LOGGER.debug("sending user transaction message for twitter-id='{}', topic='usertransactiona'", crawledUser.getTwitterID());

        if (crawledUser.getUsercrawled().isEmpty()){
            crawledUser.setUsercrawlstatus(Crawler.NOTHING_TO_SYNC);
        } else{
            crawledUser.setUsercrawlstatus(Crawler.SYNC_INIT);
            String payload = crawledUser.getTwitterID()+":"+crawledUser.getUsercrawled();
            kafkaTemplate.send("usertransactiona", payload);
        }
        crawledUserRepository.save(crawledUser);
    }
}