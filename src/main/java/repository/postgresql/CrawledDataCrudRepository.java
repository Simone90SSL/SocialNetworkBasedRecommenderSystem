package repository.postgresql;

import domain.CrawledData;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

public interface CrawledDataCrudRepository {

    List<CrawledData> findByCrawlstatus(int crawlstatus);

    Page<CrawledData> findAll(Pageable pageable);
}
