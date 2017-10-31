package crawler.following.frontier.consumer;

import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.TwitterCralwerFactory;
import crawler.following.TwitterFollowingCrawler;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;
import repository.neo4j.UserRepository;
import twitter4j.TwitterException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component
public class FollowingFrontierConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowingFrontierConsumer.class);

    @Autowired
    private FollowingCrawlerContextConfiguration followingCrawlerContextConfiguration;

    @Autowired
    private FollowingFrontierProducer followingFrontierProducer;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @KafkaListener(topics = "${kafka.topic.followingfrontier}")
    public void receive(String TwitterId) {
        LOGGER.info("Getting twitter user from the frontier with twitter-id='{}'", TwitterId);

        // Check that twitter user is not present, or it has been crawled at least 24h ago
        CrawledUser crawledUser = crawledUserRepository.findOne(Long.parseLong(TwitterId));

        if (crawledUser == null){
            // User has never been crawled
            LOGGER.info("User with twitter-id='{}' haas never been crawled --> must be created", TwitterId);
            crawledUser = new CrawledUser();
            crawledUser.setTwitterID(Long.parseLong(TwitterId));
        } else if (crawledUser.getLastFollowingCrawl() != null){
            // User is in the DB --> check if it must be crawled
            long diff = (new Date()).getTime() - crawledUser.getLastFollowingCrawl().getTime();
            if (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) <= 7){
                LOGGER.info("followinf of twitter-id='{}' has recently been crawled --> SKIP IT", TwitterId);
                return;
            }
        }
        crawledUser.setStatus(1);
        crawledUserRepository.save(crawledUser);

        // User must be crawled
        LOGGER.info("User with twitter-id='{}' will be crawled", TwitterId);
        try {
            TwitterFollowingCrawler twitterFollowingCrawler = TwitterCralwerFactory.getTwitterCrawler(
                    followingCrawlerContextConfiguration,
                    followingFrontierProducer,
                    userRepository,
                    crawledUserRepository);
            twitterFollowingCrawler.crawlFollowedByTwitterUserId(Long.parseLong(TwitterId));
        } catch (TwitterException te){
            te.printStackTrace();
        }
    }
}