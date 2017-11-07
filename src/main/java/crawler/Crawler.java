package crawler;

public interface Crawler {

    public static final int CRAWLING_INIT = 0;
    public static  final int CRAWLING_WAITING = 1;
    public static final int CRAWLING_RUN = 2;
    public static final int CRAWLING_END = 3;
    public static final int CRAWLING_ERROR = 7;
    public static final int CRAWLING_NOT_FOUND = 9;
    public static final int SYNC_INIT = 4;
    public static final int SYNC_TERMINATED = 5;
    public static final int SYNC_TERMINATED_ERR = 6;
    public static final int NOTHING_TO_SYNC = 8;

    public static final int MAX_RETRY = 3;
}
