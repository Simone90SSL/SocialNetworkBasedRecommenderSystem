package crawler.user.frontier;

import crawler.TwitterCrawler;
import domain.CrawledUser;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import transaction.user.producer.UserTransactionProducer;
import twitter4j.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

public class TwitterUserCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterUserCrawler.class);

    private CrawledUserRepository crawledUserRepository;
    private UserTransactionProducer userTransactionProducer;


    public TwitterUserCrawler(
            UserCrawlerContextConfiguration userCrawlerContextConfiguration,
            CrawledUserRepository crawledUserRepository,
            UserTransactionProducer userTransactionProducer)
            throws TwitterException{

        super(userCrawlerContextConfiguration);
        this.crawledUserRepository = crawledUserRepository;
        this.userTransactionProducer = userTransactionProducer;
    }

    public synchronized void crawlUser(CrawledUser crawledUser){

        boolean retry = false;
        int number_retry = 0;
        int secondsUntilReset;

        try {
            crawledUser.setLastUserCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setUsercrawlstatus(TwitterCrawler.CRAWLING_RUN);
            crawledUserRepository.save(crawledUser);

            LOGGER.info("Start gathering information of the user '{}'", crawledUser.getTwitterID());
            User currentTwitterUser = null;
            do{
                try {
                    currentTwitterUser = twitter.showUser(crawledUser.getTwitterID());
                    retry = false;
                } catch (TwitterException te){
                    if (( te.getStatusCode() == 429 || te.getStatusCode() == 403 )
                            && number_retry < TwitterCrawler.MAX_RETRY){
                        retry = true;
                        number_retry++;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else if(te.getStatusCode() == 404){
                        // The URI requested is invalid or the resource requested, such as a user, does not exists.
                        // Also returned when the requested format is not supported by the requested method.
                        crawledUser.setUsercrawlstatus(TwitterCrawler.CRAWLING_NOT_FOUND);
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

            crawledUser.setUsercrawlstatus(TwitterCrawler.CRAWLING_END);
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

        crawledUser.setUsercrawlstatus(TwitterCrawler.CRAWLING_ERROR);
        crawledUserRepository.save(crawledUser);
    }
}
