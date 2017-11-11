package crawler.following;

import crawler.TwitterCrawler;
import crawler.user.frontier.producer.UserFrontierProducer;
import domain.CrawledUser;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import transaction.following.producer.FollowingTransactionProducer;
import twitter4j.Twitter;
import twitter4j.IDs;
import twitter4j.User;
import twitter4j.TwitterFactory;
import twitter4j.TwitterException;
import twitter4j.auth.AccessToken;

import java.util.Date;

public class TwitterFollowingCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterFollowingCrawler.class);

    private UserFrontierProducer userFrontierProducer;
    private CrawledUserRepository crawledUserRepository;
    private FollowingTransactionProducer followingTransactionProducer;

    public TwitterFollowingCrawler(
            FollowingCrawlerContextConfiguration followingCrawlerContextConfiguration,
            UserFrontierProducer userFrontierProducer,
            CrawledUserRepository crawledUserRepository,
            FollowingTransactionProducer followingTransactionProducer)
            throws TwitterException{
        super(followingCrawlerContextConfiguration);
        this.userFrontierProducer = userFrontierProducer;
        this.crawledUserRepository = crawledUserRepository;
        this.followingTransactionProducer = followingTransactionProducer;
    }

    public synchronized void crawlFollowedByTwitterUserId(long TwitterId){

        boolean retry;
        int number_retry = 0;
        int secondsUntilReset;
        CrawledUser crawledUser = null;

        try {
            crawledUser = crawledUserRepository.findOne(TwitterId);
            crawledUser.setLastFollowingCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setFollowingcrawlstatus(TwitterCrawler.CRAWLING_RUN);
            crawledUserRepository.save(crawledUser);

            LOGGER.info("Start gathering following of the user '{}'", TwitterId);
            long cursor = -1;
            IDs followed = null;
            String followingCrawled = "";
            // Iterate over the followers
            do {
                do{
                    try {
                        followed = twitter.getFriendsIDs(TwitterId, cursor);
                        retry = false;
                    } catch (TwitterException te){
                        if (te.getStatusCode() == 429 && number_retry < TwitterCrawler.MAX_RETRY){
                            retry = true;
                            number_retry++;
                            secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                            LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                            Thread.sleep(secondsUntilReset*1000);
                            LOGGER.info("WAKE UP and RETRY");
                        } else{
                            throw te;
                        }
                    }
                }while (retry);

                for (long f: followed.getIDs()){
                    followingCrawled+=f+",";

                    // Adding the user found to the FRONTIER
                    LOGGER.debug("Add twitter user id into the frontier, with twitter-id='{}'", f);
                    userFrontierProducer.send("" + f);
                }
            } while ((cursor = followed.getNextCursor()) != 0);

            crawledUser.setFollowingcrawlstatus(TwitterCrawler.CRAWLING_END);
            crawledUser.setFollowingcrawled(followingCrawled);
            crawledUserRepository.save(crawledUser);

            // Send request to syinchronize the social graph microservice
            followingTransactionProducer.send(TwitterId+","+followingCrawled);
            crawledUser.setFollowingcrawlstatus(TwitterCrawler.SYNC_INIT);
            crawledUserRepository.save(crawledUser);
            return;
        } catch (TwitterException te){
            LOGGER.error("Twitter4J Error during twitter following crawling with input '{}'", TwitterId);
            LOGGER.error(te.getMessage(), te);
            te.printStackTrace();
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted Error during twitter following crawling with input '{}'", TwitterId);
            LOGGER.error(ie.getMessage(), ie);
            ie.printStackTrace();
        } catch (Exception e){
            LOGGER.error("Error during twitter following crawling with input '{}'", TwitterId);
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }

        crawledUser.setFollowingcrawlstatus(TwitterCrawler.CRAWLING_ERROR);
        crawledUserRepository.save(crawledUser);
    }
}
