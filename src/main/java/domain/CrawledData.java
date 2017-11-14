package domain;

import java.sql.Date;

public interface CrawledData {

    public abstract long getTwitterID();

    public abstract Date getLastCrawl();

    public abstract int getCrawlStatus();

    public abstract String getDataCrawled();

    public abstract void setCrawlStatus(int status);

    public void setLastCrawl(Date lastCrawl);

    public void setDataCrawled(String dataCrawled);

    public abstract boolean isDataCrawledEmpty();
}

