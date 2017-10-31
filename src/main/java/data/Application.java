package data;

import crawler.following.frontier.producer.FollowingFrontierProducer;
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
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories;

import java.util.Arrays;


/**
 * Created by simonecaldaro on 11/09/2017.
 */
@SpringBootApplication
@ComponentScan({"controller", "data", "crawler", "repository"})
@EntityScan("domain")
@EnableNeo4jRepositories("repository.neo4j")
@EnableJpaRepositories("repository.postgresql")
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private FollowingFrontierProducer followingFrontierProducer;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            System.out.println("Let's inspect the beans provided by Spring Boot:");
            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

            long[] seed = {156645608L, 8143682L, 16665197L, 17157238L};
            for (long twitterId: seed){
                followingFrontierProducer.send(""+twitterId);
            }
        };

    }
}
