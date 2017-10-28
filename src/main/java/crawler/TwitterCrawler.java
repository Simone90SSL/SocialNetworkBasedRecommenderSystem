package crawler;

import crawler.user.frontier.consumer.UserFrontierConsumer;
import crawler.user.frontier.producer.UserFrontierProducer;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;
import twitter4j.IDs;
import twitter4j.User;
import twitter4j.TwitterFactory;
import twitter4j.TwitterException;
import twitter4j.auth.AccessToken;

import java.util.ArrayList;
import java.util.List;

public class TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(UserFrontierConsumer.class);

    private UserFrontierProducer userFrontierProducer;

    private Twitter twitter;


    public TwitterCrawler(CrawlerContextConfiguarion conf, UserFrontierProducer userFrontierProducer) throws TwitterException{
        this.userFrontierProducer = userFrontierProducer;

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

    public List<domain.User> getFollowed(long userId) throws TwitterException {
        ArrayList<domain.User> followedList = new ArrayList<domain.User>();

        IDs followed = twitter.getFriendsIDs(userId);
        domain.User userFollowed;
        User u;
        String uLocation;
        String uName;
        String uScreenName;
        String uUrl;
        for (long followedId: followed.getIDs()){

            // Adding the user found to the FRONTIER
            userFrontierProducer.send("userfrontier", ""+followedId);

            // Saving the user to the DB
            u = twitter.showUser(followedId);
            uLocation = u.getLocation();
            uName = u.getName();
            uScreenName = u.getScreenName();
            uUrl = u.getURL();

            userFollowed = new domain.User(followedId, uName, uScreenName, uLocation, uUrl);
            followedList.add(userFollowed);
        }

        return followedList;
    }

    public synchronized void crawlFollowedByUserId(long TwitterId){
        try {
            LOGGER.info("Start gathering follower of the user with twitter-id='{}'", TwitterId);
            long cursor = -1;
            IDs followed = twitter.getFriendsIDs(TwitterId, cursor);
            do {
                domain.User userFollowed;
                User u;
                String uLocation;
                String uName;
                String uScreenName;
                String uUrl;

                long[] followers = followed.getIDs();
                LOGGER.info("twitter-id='{}' - found '{}' followers", TwitterId, followers.length);
                for (long followedId: followers){

                    // Saving the user to the DB
                    u = twitter.showUser(followedId);
                    uLocation = u.getLocation();
                    uName = u.getName();
                    uScreenName = u.getScreenName();
                    uUrl = u.getURL();

                    userFollowed = new domain.User(followedId, uName, uScreenName, uLocation, uUrl);
                    LOGGER.info("'{}' has follower '{}'", TwitterId, userFollowed);

                    // Adding the user found to the FRONTIER
                    LOGGER.info("Add twitter user id into the frontier, with twitter-id='{}'", followedId);
                    userFrontierProducer.send("userfrontier", "" + followedId);
                }

            } while (followed.hasNext());

            Thread.sleep(10000);
        } catch (TwitterException te){
            te.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
