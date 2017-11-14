package crawler.user;

import crawler.CrawlerContextConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value = "classpath:application.properties")
public class UserCrawlerContextConfiguration implements CrawlerContextConfiguration{

    @Value("${consumerKey}")
    private String consumerKey;

    @Value("${consumerSecret}")
    private String consumerSecret;

    @Value("${accessToken}")
    private String accessToken;

    @Value("${accessTokenSecret}")
    private String accessTokenSecret;

    public String getName(){
        return "UserCrawlerContextConfiguration";
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
