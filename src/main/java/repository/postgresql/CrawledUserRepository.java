package repository.postgresql;

import domain.CrawledFollowing;
import domain.CrawledTweets;
import domain.CrawledUser;
import org.springframework.data.repository.CrudRepository;

public interface CrawledUserRepository extends CrudRepository<CrawledUser, Long>, CrawledDataCrudRepository {

}
