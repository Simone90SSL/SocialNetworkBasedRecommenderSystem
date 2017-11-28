package data;

import crawler.following.FollowingCrawlerContextConfiguration;
import frontier.FrontierProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;

import java.util.Arrays;

/**
 * Created by simonecaldaro on 11/09/2017.
 */
@SpringBootApplication
@ComponentScan({"controller", "data", "crawler", "repository", "frontier", "synchronization"})
@EntityScan("domain")
@EnableJpaRepositories("repository.postgresql")
@EnableWebMvc
@EnableAutoConfiguration
public class Application extends SpringBootServletInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetsRepository;

    @Autowired
    private FrontierProducer frontierProducer;

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

            //TwitterFollowingCrawler tfc = TwitterCralwerFactory.getTwitterFollowingCrawler(conf, fp, crt);




            //List<Object> o = crawledTweetsRepository.getStatus();
            //System.out.println(o);
            /*Page<CrawledData> l = crawledFollowingRepository.findByCrawlstatus(4, new PageRequest(page, size));
            while(l.hasNext()){
                System.out.println("Page "+(page-1));
                for (CrawledData c: l){
                    if(c.getCrawlStatus() == 4) {
                        frontierProducer.sendFollowingTransaction((CrawledFollowing) c);
                    }
                }
                l = crawledFollowingRepository.findAll(new PageRequest(page++, size));
            }*/

        };
    }
}