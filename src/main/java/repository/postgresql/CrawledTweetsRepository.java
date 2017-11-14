package repository.postgresql;

import domain.CrawledFollowing;
import domain.CrawledTweets;
import org.springframework.data.repository.CrudRepository;

public interface CrawledTweetsRepository extends CrudRepository<CrawledTweets, Long>, CrawledDataCrudRepository {

}
