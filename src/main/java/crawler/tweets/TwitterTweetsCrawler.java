package crawler.tweets;

import crawler.Crawler;
import domain.CrawledUser;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import transaction.tweets.producer.TweetsTransactionProducer;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class TwitterTweetsCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterTweetsCrawler.class);

    private CrawledUserRepository crawledUserRepository;
    private TweetsTransactionProducer tweetsTransactionProducer;
    private Twitter twitter;


    public TwitterTweetsCrawler(
            TweetsCrawlerContextConfiguration conf,
            CrawledUserRepository crawledUserRepository,
            TweetsTransactionProducer tweetsTransactionProducer)
            throws TwitterException{

        this.crawledUserRepository = crawledUserRepository;
        this.tweetsTransactionProducer = tweetsTransactionProducer;

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

    public synchronized void crawlTweets(CrawledUser crawledUser){

        boolean retry;
        int number_retry = 0;
        int secondsUntilReset;

        try {
            crawledUser.setLastTweetsCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setTweetscrawlstatus(Crawler.CRAWLING_RUN);
            crawledUserRepository.save(crawledUser);

            LOGGER.info("Start gathering tweets of the user '{}'", crawledUser.getTwitterID());
            List<Status> statuses = null;
            do{
                try {
                    Paging paging = new Paging(1, 200);
                    statuses = twitter.getUserTimeline(crawledUser.getTwitterID(), paging);
                    retry = false;
                } catch (TwitterException te){
                    if (te.getStatusCode() == 429
                            && number_retry < Crawler.MAX_RETRY){
                        retry = true;
                        number_retry++;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else if(te.getStatusCode() == 404 || te.getStatusCode() == 401){
                        // The URI requested is invalid or the resource requested, such as a user, does not exists.
                        // Also returned when the requested format is not supported by the requested method.
                        crawledUser.setTweetscrawlstatus(Crawler.CRAWLING_NOT_FOUND);
                        crawledUserRepository.save(crawledUser);
                        return;
                    } else{
                        throw te;
                    }
                }
            }while (retry);

            String text;
            Date createdAt;
            String geoLocation;
            String lang;
            long id;
            String twitterTweetsJSON = "[";
            for (Status s: statuses){
                text = Optional.ofNullable(s.getText()).orElse("");
                createdAt = s.getCreatedAt();
                geoLocation = s.getGeoLocation()!=null?s.getGeoLocation().toString():"";
                id = s.getId();
                lang = Optional.ofNullable(s.getLang()).orElse("");

                // Saving the crawled user
                HashMap<String, String> twitterTweetsMap = new HashMap<>();
                twitterTweetsMap.put("TEXT", text);
                twitterTweetsMap.put("CREATEDAT", createdAt.toString());
                twitterTweetsMap.put("GEOLOCATION", geoLocation.toString());
                twitterTweetsMap.put("ID", ""+id);
                twitterTweetsMap.put("LANG", lang);

                twitterTweetsJSON += new JSONObject(twitterTweetsMap).toString()+",";
            }
            twitterTweetsJSON += "]";


            crawledUser.setTweetscrawlstatus(Crawler.CRAWLING_END);
            crawledUser.setTweetscrawled(twitterTweetsJSON);
            crawledUserRepository.save(crawledUser);
            tweetsTransactionProducer.send(crawledUser);
            return;
        } catch (TwitterException te){
            LOGGER.error("Twitter4J Error during twitter TWEETS crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(te.getMessage(), te);
            te.printStackTrace();
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted Error during twitter TWEETS crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(ie.getMessage(), ie);
            ie.printStackTrace();
        } catch (Exception e){
            LOGGER.error("Error during twitter TWEETS crawling with input '{}'", crawledUser.getTwitterID());
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }

        crawledUser.setTweetscrawlstatus(Crawler.CRAWLING_ERROR);
        crawledUserRepository.save(crawledUser);
    }
}
