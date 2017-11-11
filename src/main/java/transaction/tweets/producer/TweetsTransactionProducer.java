package transaction.tweets.producer;

import crawler.TwitterCrawler;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;

@Component
public class TweetsTransactionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetsTransactionProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    public void send(CrawledUser crawledUser) {
        LOGGER.info("sending tweets of user id='{}' to topic='tweetstransactiona'", crawledUser.getTwitterID());

        String tweets = crawledUser.getTweetscrawled();
        if (tweets == null || tweets.isEmpty() || "[]".equals(crawledUser.getTweetscrawled())){
            crawledUser.setTweetscrawlstatus(TwitterCrawler.NOTHING_TO_SYNC);
        } else{
            crawledUser.setTweetscrawlstatus(TwitterCrawler.SYNC_INIT);
            String payload = crawledUser.getTwitterID()+":"+crawledUser.getTweetscrawled();
            kafkaTemplate.send("tweetstransactiona", payload);
        }
        crawledUserRepository.save(crawledUser);
    }
}