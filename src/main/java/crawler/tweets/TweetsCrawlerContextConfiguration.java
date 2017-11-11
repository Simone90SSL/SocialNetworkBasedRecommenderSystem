package crawler.tweets;

import crawler.CrawlerContextConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value = "classpath:application.properties")
public class TweetsCrawlerContextConfiguration implements CrawlerContextConfiguration {

    @Value("${crawl.tweets.consumerKey}")
    private String consumerKey;

    @Value("${crawl.tweets.consumerSecret}")
    private String consumerSecret;

    @Value("${crawl.tweets.accessToken}")
    private String accessToken;

    @Value("${crawl.tweets.accessTokenSecret}")
    private String accessTokenSecret;

    @Override
    public String getName() {
        return "TweetsCrawlerContextConfiguration";
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getAccessTokenSecret() {
        return accessTokenSecret;
    }
}
