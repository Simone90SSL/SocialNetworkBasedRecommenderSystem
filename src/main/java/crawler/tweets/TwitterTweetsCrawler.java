package crawler.tweets;

import crawler.TwitterCrawler;
import crawler.tweets.producer.TweetsFrontierProducer;
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

public class TwitterTweetsCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterTweetsCrawler.class);

    private CrawledUserRepository crawledUserRepository;
    private TweetsTransactionProducer tweetsTransactionProducer;
    private TweetsFrontierProducer tweetsFrontierProducer;

    public TwitterTweetsCrawler(
            TweetsCrawlerContextConfiguration tweetsCrawlerContextConfiguration,
            CrawledUserRepository crawledUserRepository,
            TweetsTransactionProducer tweetsTransactionProducer,
            TweetsFrontierProducer tweetsFrontierProducer)
            throws TwitterException{

        super(tweetsCrawlerContextConfiguration);

        this.crawledUserRepository = crawledUserRepository;
        this.tweetsTransactionProducer = tweetsTransactionProducer;
        this.tweetsFrontierProducer = tweetsFrontierProducer;
    }

    public synchronized void crawlTweets(CrawledUser crawledUser){

        boolean retry;
        int number_retry = 0;
        int secondsUntilReset;

        try {
            crawledUser.setLastTweetsCrawl(new java.sql.Date(new Date().getTime()));
            crawledUser.setTweetscrawlstatus(TwitterCrawler.CRAWLING_RUN);
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
                            && number_retry < TwitterCrawler.MAX_RETRY){
                        retry = true;
                        number_retry++;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception caused by 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else if(te.getStatusCode() == 404 || te.getStatusCode() == 401){
                        // The URI requested is invalid or the resource requested, such as a user, does not exists.
                        // Also returned when the requested format is not supported by the requested method.
                        crawledUser.setTweetscrawlstatus(TwitterCrawler.CRAWLING_NOT_FOUND);
                        crawledUserRepository.save(crawledUser);
                        return;
                    } else if(te.getStatusCode() == 130){
                        // The Twitter servers are up, but overloaded with requests. Try again later.
                        crawledUser.setTweetscrawlstatus(TwitterCrawler.CRAWLING_ERROR);
                        crawledUserRepository.save(crawledUser);
                        tweetsFrontierProducer.send(""+crawledUser.getTwitterID());
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


            crawledUser.setTweetscrawlstatus(TwitterCrawler.CRAWLING_END);
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

        crawledUser.setTweetscrawlstatus(TwitterCrawler.CRAWLING_ERROR);
        crawledUserRepository.save(crawledUser);
    }
}
