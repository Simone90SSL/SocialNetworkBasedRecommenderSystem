package crawler.user.frontier.consumer;

import crawler.Crawler;
import crawler.TwitterCralwerFactory;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import crawler.tweets.producer.TweetsFrontierProducer;
import crawler.user.frontier.UserCrawlerContextConfiguration;
import crawler.user.frontier.TwitterUserCrawler;
import crawler.user.frontier.producer.UserFrontierProducer;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;
import transaction.user.producer.UserTransactionProducer;
import twitter4j.TwitterException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component
public class UserFrontierConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserFrontierConsumer.class);

    @Autowired
    private UserCrawlerContextConfiguration userCrawlerContextConfiguration;

    @Autowired
    private UserFrontierProducer userFrontierProducer;
    @Autowired
    private FollowingFrontierProducer followingFrontierProducer;
    @Autowired
    private TweetsFrontierProducer tweetsFrontierProducer;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @Autowired
    private UserTransactionProducer userTransactionProducer;

    @KafkaListener(topics = "${kafka.topic.userfrontier}")
    public void receive(String TwitterId) {
        LOGGER.info("Getting twitter user from the frontier with twitter-id='{}'", TwitterId);

        // Trigger FOLLOWING crawling
        followingFrontierProducer.send(TwitterId);
        // Trigger TWEETS crawling
        tweetsFrontierProducer.send(TwitterId);

        // Check that twitter user is not present, or it has been crawled at least 24h ago
        CrawledUser crawledUser = crawledUserRepository.findOne(Long.parseLong(TwitterId));

        if (crawledUser == null) {
            // User has never been crawled
            LOGGER.info("User with twitter-id='{}' has never been crawled --> must be created", TwitterId);
            crawledUser = new CrawledUser();
            crawledUser.setTwitterID(Long.parseLong(TwitterId));
        } else if (crawledUser.getLastUserCrawl() != null){
            // User is in the DB --> check if it must be crawled
            long diff = (new Date()).getTime() - crawledUser.getLastUserCrawl().getTime();

            if (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) <= 7){
                LOGGER.info("user of twitter-id='{}' has recently been crawled --> SKIP IT", TwitterId);
                return;
            }

            //if(crawledUser.getUsercrawlstatus() != Crawler.SYNC_TERMINATED){
            //    LOGGER.info("User with twitter-id='{}' is in a state that cannot be crawled --> exit", TwitterId);
            //    return;
            //}
        }

        crawledUser.setUsercrawlstatus(Crawler.CRAWLING_WAITING);
        crawledUserRepository.save(crawledUser);

        // User must be crawled
        LOGGER.info("User with twitter-id='{}' will be crawled", TwitterId);
        try {
            TwitterUserCrawler twitterUserCrawler = TwitterCralwerFactory.getTwitterUserCrawler(
                    userCrawlerContextConfiguration,
                    crawledUserRepository,
                    userTransactionProducer);
            twitterUserCrawler.crawlUser(crawledUser);
        } catch (TwitterException te){
            te.printStackTrace();
        }
    }


}