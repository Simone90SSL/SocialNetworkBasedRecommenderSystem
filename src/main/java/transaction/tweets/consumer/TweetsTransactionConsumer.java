package transaction.tweets.consumer;

import crawler.Crawler;
import domain.CrawledUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledUserRepository;

@Component
public class TweetsTransactionConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetsTransactionConsumer.class);

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @KafkaListener(topics = "${kafka.topic.tweetstransactionb}")
    public void receive(String transactionResponse) {
        LOGGER.info("Transaction terminated '{}'", transactionResponse);

        long TwitterId = Long.parseLong(transactionResponse.split(",")[0]);
        String status = transactionResponse.split(",")[1];

        CrawledUser crawledUser = crawledUserRepository.findOne(TwitterId);
        if (status.equals("OK")){
            crawledUser.setTweetscrawlstatus(Crawler.SYNC_TERMINATED);
            //crawledUser.setFollowingcrawled("");
        } else{
            crawledUser.setTweetscrawlstatus(Crawler.SYNC_TERMINATED_ERR);
        }

        crawledUserRepository.save(crawledUser);
    }


}