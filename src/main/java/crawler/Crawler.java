package crawler;

public interface Crawler {

    public final int CRAWLING_INIT = 0;
    public final int CRAWLING_WAITING = 1;
    public final int CRAWLING_RUN = 2;
    public final int CRAWLING_END = 3;
    public final int SYNC_INIT = 4;
    public final int SYNC_TERMINATED = 5;
    public final int SYNC_TERMINATED_ERR = 6;
}
