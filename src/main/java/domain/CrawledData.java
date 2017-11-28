package domain;

import java.sql.Date;

public interface CrawledData {

    public long getTwitterID();

    public Date getLastCrawl();

    public int getCrawlStatus();

    public String getDataCrawled();

    public void setCrawlStatus(int status);

    public void setLastCrawl(Date lastCrawl);

    public void setDataCrawled(String dataCrawled);

    public boolean isDataCrawledEmpty();
}

