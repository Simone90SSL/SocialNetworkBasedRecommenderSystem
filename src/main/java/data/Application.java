package data;

import crawler.TwitterCralwerFactory;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.tweets.TwitterTweetsCrawler;
import crawler.user.frontier.TwitterUserCrawler;
import crawler.user.frontier.UserCrawlerContextConfiguration;
import domain.CrawledUser;
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
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import repository.postgresql.CrawledUserRepository;
import transaction.tweets.producer.TweetsTransactionProducer;
import transaction.user.producer.UserTransactionProducer;

import java.util.Arrays;

/**
 * Created by simonecaldaro on 11/09/2017.
 */
@SpringBootApplication
@ComponentScan({"controller", "data", "crawler", "repository", "transaction"})
@EntityScan("domain")
@EnableJpaRepositories("repository.postgresql")
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private CrawledUserRepository cr;

    @Autowired
    TweetsCrawlerContextConfiguration conf;
    @Autowired
    CrawledUserRepository crawledUserRepository;
    @Autowired
    TweetsTransactionProducer tweetsTransactionProducer;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            System.out.println("Let's inspect the beans provided by Spring Boot:");
            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

            TwitterTweetsCrawler tuc = TwitterCralwerFactory.getTwitterTweetsCrawler(
                    conf, crawledUserRepository, tweetsTransactionProducer
            );

            for(CrawledUser c: cr.findByTweetscrawlstatus(4)){
                tweetsTransactionProducer.send(c);
            }
        };
    }
}
