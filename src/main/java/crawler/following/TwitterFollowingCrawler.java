package crawler.following;

import crawler.following.frontier.consumer.FollowingFrontierConsumer;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import domain.CrawledUser;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import repository.neo4j.UserRepository;
import twitter4j.Twitter;
import twitter4j.IDs;
import twitter4j.User;
import twitter4j.TwitterFactory;
import twitter4j.TwitterException;
import twitter4j.auth.AccessToken;

import java.util.Date;

public class TwitterFollowingCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FollowingFrontierConsumer.class);

    private FollowingFrontierProducer followingFrontierProducer;
    private CrawledUserRepository crawledUserRepository;
    private UserRepository userRepository;
    private Twitter twitter;


    public TwitterFollowingCrawler(
            FollowingCrawlerContextConfiguration conf,
            FollowingFrontierProducer followingFrontierProducer,
            UserRepository userRepository,
            CrawledUserRepository crawledUserRepository)
            throws TwitterException{
        this.followingFrontierProducer = followingFrontierProducer;
        this.userRepository = userRepository;
        this.crawledUserRepository = crawledUserRepository;

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

        try {
            CrawledUser crawledUser = crawledUserRepository.findOne(TwitterId);
            crawledUser.setLastFollowingCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setStatus(2);
            crawledUserRepository.save(crawledUser);

            domain.User currentUser = userRepository.findByTwitterId(TwitterId);
            if (currentUser == null){
                // Get information of the user --> insert into the DB
                User currentTwitterUser = null;
                //currentTwitterUser = twitter.showUser(TwitterId);

                currentUser = new domain.User(TwitterId);
                this.userRepository.save(currentUser);
                currentUser = this.userRepository.findByTwitterId(TwitterId);
            }

            LOGGER.info("Start gathering following of the user ", currentUser);
            long cursor = -1;
            int checkSumFollowing = 0;
            IDs followed = null;
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

                domain.User userFollowed;

                long[] followers = followed.getIDs();
                LOGGER.info("twitter-id='{}' - found '{}' followers", TwitterId, followers.length);
                for (long followedId: followers){

                    checkSumFollowing ++;

                    // Get information of the user --> insert into the DB
                    // twitterUserFollowed = twitter.showUser(followedId);

                    userFollowed = userRepository.findByTwitterId(followedId);
                    if (userFollowed == null){
                        userFollowed = new domain.User(
                                followedId);
                        this.userRepository.save(userFollowed);
                        userFollowed = this.userRepository.findByTwitterId(followedId);
                    }
                    LOGGER.info("'{}' has following '{}'", TwitterId, userFollowed);
                    currentUser.follow(userFollowed);

                    LOGGER.info("'{}' vs '{}'", checkSumFollowing, currentUser.follows.size());
                    if (currentUser.follows.size() != checkSumFollowing){
                        LOGGER.error("Something wrong with cursor '{}' : twitter Id '{}'", cursor, TwitterId);
                        break;
                    }

                    // Adding the user found to the FRONTIER
                    LOGGER.debug("Add twitter user id into the frontier, with twitter-id='{}'", followedId);
                    followingFrontierProducer.send("" + followedId);
                }
            } while ((cursor = followed.getNextCursor()) != 0);

            // Saving the crawled user
            this.userRepository.save(currentUser);
            crawledUser.setStatus(0);
            crawledUserRepository.save(crawledUser);
        } catch (TwitterException te){
            te.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
