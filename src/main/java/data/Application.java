package data;

import crawler.TwitterCralwerFactory;
import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.following.TwitterFollowingCrawler;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.tweets.TwitterTweetsCrawler;
import domain.CrawledData;
import domain.CrawledFollowing;
import frontier.consumer.FrontierConsumer;
import frontier.producer.FrontierProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;

import java.util.Arrays;

/**
 * Created by simonecaldaro on 11/09/2017.
 */
@SpringBootApplication
@ComponentScan({"controller", "data", "crawler", "repository", "frontier"})
@EntityScan("domain")
@EnableJpaRepositories("repository.postgresql")
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private CrawledFollowingRepository crt;

    @Autowired
    private FrontierConsumer fp;

    @Autowired
    private FollowingCrawlerContextConfiguration conf;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            System.out.println("Let's inspect the beans provided by Spring Boot:");
            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

//            TwitterFollowingCrawler tfc = TwitterCralwerFactory.getTwitterFollowingCrawler(conf, fp, crt);
//
//            int page = 0;
//            int size = 100;
//            Page<CrawledData> l = crt.findAll(new PageRequest(page++, size));
//            while(l.hasNext()){
//                for (CrawledData c: l){
//                    if (c.getCrawlStatus() != 4 && c.getCrawlStatus() != 5){
//                        tfc.crawlFollowedByTwitterUserId(c.getTwitterID());
//                    }
//                }
//                l = crt.findAll(new PageRequest(page++, size));
//            }

            fp.receiveFollowing("-1");
        };
    }
}
