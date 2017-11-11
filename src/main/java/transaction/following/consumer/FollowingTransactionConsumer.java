package transaction.following.consumer;

import crawler.TwitterCrawler;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;

@Component
public class FollowingTransactionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FollowingTransactionConsumer.class);

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @KafkaListener(topics = "${kafka.topic.followingtransactionb}")
    public void receive(String transactionResponse) {
        LOGGER.info("Transaction terminated '{}'", transactionResponse);

        long TwitterId = Long.parseLong(transactionResponse.split(",")[0]);
        String status = transactionResponse.split(",")[1];

        CrawledUser crawledUser = crawledUserRepository.findOne(TwitterId);
        if (status.equals("OK")){
            crawledUser.setFollowingcrawlstatus(TwitterCrawler.SYNC_TERMINATED);
            //crawledUser.setFollowingcrawled("");
        } else{
            crawledUser.setFollowingcrawlstatus(TwitterCrawler.SYNC_TERMINATED_ERR);
        }

        crawledUserRepository.save(crawledUser);
    }


}