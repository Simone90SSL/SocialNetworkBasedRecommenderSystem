package repository.neo4j;

import domain.User;
import org.springframework.data.neo4j.repository.GraphRepository;

public interface UserRepository extends GraphRepository<User> {

    User findByTwitterId(long twitterId);
    Iterable<User> findByName(String name);

}
