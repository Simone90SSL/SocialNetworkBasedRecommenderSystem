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

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterUserCrawler.class);

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

    public synchronized void crawlUser(CrawledUser crawledUser){

        boolean retry = false;
        int number_retry = 0;
        int secondsUntilReset;

        try {
            crawledUser.setLastUserCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setUsercrawlstatus(Crawler.CRAWLING_RUN);
            crawledUserRepository.save(crawledUser);

            LOGGER.info("Start gathering information of the user '{}'", crawledUser.getTwitterID());
            User currentTwitterUser = null;
            do{
                try {
                    currentTwitterUser = twitter.showUser(crawledUser.getTwitterID());
                    retry = false;
                } catch (TwitterException te){
                    if (( te.getStatusCode() == 429 || te.getStatusCode() == 403 )
                            && number_retry < Crawler.MAX_RETRY){
                        retry = true;
                        number_retry++;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else if(te.getStatusCode() == 404){
                        // The URI requested is invalid or the resource requested, such as a user, does not exists.
                        // Also returned when the requested format is not supported by the requested method.
                        crawledUser.setUsercrawlstatus(Crawler.CRAWLING_NOT_FOUND);
                        crawledUserRepository.save(crawledUser);
                        return;
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
            LOGGER.debug("Init transaction for user information '{}'", crawledUser.getTwitterID());

            userTransactionProducer.send(crawledUser);
            return;
        } catch (TwitterException te){
            LOGGER.error("Twitter4J Error during twitter USER crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(te.getMessage(), te);
            te.printStackTrace();
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted Error during twitter USER crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(ie.getMessage(), ie);
            ie.printStackTrace();
        } catch (Exception e){
            LOGGER.error("Error during twitter USER crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }

        crawledUser.setUsercrawlstatus(Crawler.CRAWLING_ERROR);
        crawledUserRepository.save(crawledUser);
    }
}
