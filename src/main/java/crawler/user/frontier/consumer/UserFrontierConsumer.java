package crawler.user.frontier.consumer;

import crawler.CrawlerContextConfiguarion;
import crawler.TwitterCralwerFactory;
import crawler.TwitterCrawler;
import crawler.user.frontier.producer.UserFrontierProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

@Component
public class UserFrontierConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserFrontierConsumer.class);

    @Autowired
    private CrawlerContextConfiguarion conf;

    @Autowired
    private UserFrontierProducer userFrontierProducer;

    @KafkaListener(topics = "${kafka.topic.userfrontier}")
    public void receive(String TwitterId) {
        LOGGER.info("Getting twitter user from the frontier with twitter-id='{}'", TwitterId);
        try {
            Thread.sleep(1000);
            TwitterCrawler twitterCrawler = TwitterCralwerFactory.getTwitterCrawler(conf, userFrontierProducer);
            twitterCrawler.crawlFollowedByUserId(Long.parseLong(TwitterId));
        } catch (TwitterException te){
            te.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}