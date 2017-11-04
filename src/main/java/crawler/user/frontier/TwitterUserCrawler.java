package crawler.user.frontier;

import crawler.Crawler;
import crawler.user.frontier.consumer.UserFrontierConsumer;
import crawler.user.frontier.producer.UserFrontierProducer;
import domain.CrawledUser;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import transaction.user.producer.UserTransactionProducer;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

public class TwitterUserCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(UserFrontierConsumer.class);

    private CrawledUserRepository crawledUserRepository;
    private UserTransactionProducer userTransactionProducer;
    private Twitter twitter;


    public TwitterUserCrawler(
            UserCrawlerContextConfiguration conf,
            CrawledUserRepository crawledUserRepository,
            UserTransactionProducer userTransactionProducer)
            throws TwitterException{
        this.crawledUserRepository = crawledUserRepository;
        this.userTransactionProducer = userTransactionProducer;

        //Instantiate a re-usable and thread-safe factory
        TwitterFactory twitterFactory = new TwitterFactory();

        //Instantiate a new Twitter instance
        twitter = twitterFactory.getInstance();

        //setup OAuth Consumer Credentials
        twitter.setOAuthConsumer(conf.getConsumerKey(), conf.getConsumerSecret());

        //setup OAuth Access Token
        twitter.setOAuthAccessToken(new AccessToken(conf.getAccessToken(), conf.getAccessTokenSecret()));

        User u = twitter.verifyCredentials();
        if (u == null){
            throw new RuntimeException("Impossible to access on Twitter");
        }
    }

    public synchronized void crawlUserByTwitterUserId(long TwitterId){

        boolean retry;
        int secondsUntilReset;
        CrawledUser crawledUser = null;

        try {
            crawledUser = crawledUserRepository.findOne(TwitterId);
            crawledUser.setLastUserCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setUsercrawlstatus(Crawler.CRAWLING_RUN);
            crawledUserRepository.save(crawledUser);

            LOGGER.info("Start gathering information of the user '{}'", TwitterId);
            User currentTwitterUser = null;
            do{
                try {
                    currentTwitterUser = twitter.showUser(TwitterId);
                    retry = false;
                } catch (TwitterException te){
                    if (te.getStatusCode() == 429){
                        retry = true;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else{
                        throw te;
                    }
                }
            }while (retry);

            // Saving the crawled user
            HashMap<String, String> twitterUserMap = new HashMap<>();
            twitterUserMap.put("URL", Optional.ofNullable(currentTwitterUser.getURL()).orElse(""));
            twitterUserMap.put("LOCATION", Optional.ofNullable(currentTwitterUser.getLocation()).orElse(""));
            twitterUserMap.put("SCREENNAME", Optional.ofNullable(currentTwitterUser.getScreenName()).orElse(""));
            twitterUserMap.put("NAME", Optional.ofNullable(currentTwitterUser.getName()).orElse(""));
            twitterUserMap.put("EMAIL", Optional.ofNullable(currentTwitterUser.getEmail()).orElse(""));

            String twitterUserJSON = new JSONObject(twitterUserMap).toString();

            crawledUser.setUsercrawlstatus(Crawler.CRAWLING_END);
            crawledUser.setUsercrawled(twitterUserJSON);
            crawledUserRepository.save(crawledUser);

            // Send request to synchronize the user information to social graph microservice
            LOGGER.debug("Init transaction for user information '{}'", TwitterId);

            userTransactionProducer.send(TwitterId+":"+twitterUserJSON);
            crawledUser.setUsercrawlstatus(Crawler.SYNC_INIT);
            crawledUserRepository.save(crawledUser);
        } catch (TwitterException te){
            te.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
