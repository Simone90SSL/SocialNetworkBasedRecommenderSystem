package crawler.following;

import crawler.Crawler;
import crawler.following.frontier.consumer.FollowingFrontierConsumer;
import crawler.following.frontier.producer.FollowingFrontierProducer;
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

public class TwitterFollowingCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FollowingFrontierConsumer.class);

    private UserFrontierProducer userFrontierProducer;
    private CrawledUserRepository crawledUserRepository;
    private FollowingTransactionProducer followingTransactionProducer;
    private Twitter twitter;


    public TwitterFollowingCrawler(
            FollowingCrawlerContextConfiguration conf,
            UserFrontierProducer userFrontierProducer,
            CrawledUserRepository crawledUserRepository,
            FollowingTransactionProducer followingTransactionProducer)
            throws TwitterException{
        this.userFrontierProducer = userFrontierProducer;
        this.crawledUserRepository = crawledUserRepository;
        this.followingTransactionProducer = followingTransactionProducer;

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

    public synchronized void crawlFollowedByTwitterUserId(long TwitterId){

        boolean retry;
        int secondsUntilReset;
        CrawledUser crawledUser = null;

        try {
            crawledUser = crawledUserRepository.findOne(TwitterId);
            crawledUser.setLastFollowingCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setFollowingcrawlstatus(Crawler.CRAWLING_RUN);
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

                for (long f: followed.getIDs()){
                    followingCrawled+=f+",";

                    // Adding the user found to the FRONTIER
                    LOGGER.debug("Add twitter user id into the frontier, with twitter-id='{}'", TwitterId);
                    userFrontierProducer.send("" + f);
                }
            } while ((cursor = followed.getNextCursor()) != 0);

            crawledUser.setFollowingcrawlstatus(Crawler.CRAWLING_END);
            crawledUser.setFollowingcrawled(followingCrawled);
            crawledUserRepository.save(crawledUser);

            // Send request to syinchronize the social graph microservice
            followingTransactionProducer.send(TwitterId+","+followingCrawled);
            crawledUser.setFollowingcrawlstatus(Crawler.SYNC_INIT);
            crawledUserRepository.save(crawledUser);
        } catch (TwitterException te){
            te.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
