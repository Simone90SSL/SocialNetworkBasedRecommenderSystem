package transaction.user.consumer;

import crawler.TwitterCrawler;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;

@Component
public class UserTransactionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserTransactionConsumer.class);

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @KafkaListener(topics = "${kafka.topic.usertransactionb}")
    public void receive(String transactionResponse) {
        LOGGER.info("User Transaction terminated '{}'", transactionResponse);

        long TwitterId = Long.parseLong(transactionResponse.split(",")[0]);
        String status = transactionResponse.split(",")[1];

        CrawledUser crawledUser = crawledUserRepository.findOne(TwitterId);
        if (status.equals("OK")){
            crawledUser.setUsercrawlstatus(TwitterCrawler.SYNC_TERMINATED);
            //crawledUser.setUsercrawled("");
        } else{
            crawledUser.setUsercrawlstatus(TwitterCrawler.SYNC_TERMINATED_ERR);
        }

        crawledUserRepository.save(crawledUser);
    }


}