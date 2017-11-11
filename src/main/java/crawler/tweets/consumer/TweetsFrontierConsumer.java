package crawler.tweets.consumer;

import crawler.TwitterCralwerFactory;
import crawler.TwitterCrawler;
import crawler.tweets.TwitterTweetsCrawler;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.tweets.producer.TweetsFrontierProducer;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;
import transaction.tweets.producer.TweetsTransactionProducer;
import twitter4j.TwitterException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

@Component
public class TweetsFrontierConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetsFrontierConsumer.class);

    @Autowired
    private TweetsCrawlerContextConfiguration tweetsCrawlerContextConfiguration;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @Autowired
    private TweetsTransactionProducer tweetsTransactionProducer;

    @Autowired
    private TweetsFrontierProducer tweetsFrontierProducer;

    @KafkaListener(topics = "${kafka.topic.tweetsfrontier}")
    public void receive(String TwitterId) {
        LOGGER.info("Getting twitter from the frontier with twitter-id='{}'", TwitterId);

        // Check that twitter user is not present, or it has been crawled at least 24h ago
        CrawledUser crawledUser = crawledUserRepository.findOne(Long.parseLong(TwitterId));

        if (crawledUser == null) {
            // User has never been crawled
            LOGGER.info("Tweets of the user with twitter-id='{}' has never been crawled --> must be created", TwitterId);
            crawledUser = new CrawledUser();
            crawledUser.setTwitterID(Long.parseLong(TwitterId));
        } else if (crawledUser.getLastTweetsCrawl() != null){
            // User is in the DB --> check if it must be crawled
            long diff = (new Date()).getTime() - crawledUser.getLastTweetsCrawl().getTime();

            if (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) <= 1){
                LOGGER.info("Tweets of user with twitter-id='{}' has recently been crawled --> SKIP IT", TwitterId);
                return;
            }

            String lastTweetsCrawled = crawledUser.getTweetscrawled();
            if ( lastTweetsCrawled != null
                    && !lastTweetsCrawled.isEmpty()
                    && !"[]".equals(lastTweetsCrawled)
                    && crawledUser.getTweetscrawlstatus() != TwitterCrawler.SYNC_TERMINATED){
                // Before starting new crawl, need to sinc old tweets
                LOGGER.info("Tweets of the User with twitter-id='{}' must be synchronized --> exit", TwitterId);

                if (crawledUser.getTweetscrawlstatus() == TwitterCrawler.SYNC_INIT){
                    // Make a Request of sync.
                    tweetsTransactionProducer.send(crawledUser);
                } else{
                    // Request of sync already sent. Just wait
                    LOGGER.info("TWEETS-SYNC already requested for the User with twitter-id='{}' --> exit", TwitterId);
                }

                return;
            }
        }

        crawledUser.setUsercrawlstatus(TwitterCrawler.CRAWLING_WAITING);
        crawledUserRepository.save(crawledUser);

        // User must be crawled
        LOGGER.info("User with twitter-id='{}' will be crawled", TwitterId);
        try {
            TwitterTweetsCrawler twitterTweetsCrawler = TwitterCralwerFactory.getTwitterTweetsCrawler(
                    tweetsCrawlerContextConfiguration,
                    crawledUserRepository,
                    tweetsTransactionProducer,
                    tweetsFrontierProducer);
            twitterTweetsCrawler.crawlTweets(crawledUser);
        } catch (TwitterException te){
            te.printStackTrace();
        }
    }


}