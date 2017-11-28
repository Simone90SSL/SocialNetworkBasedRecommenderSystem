package data;

import crawler.TwitterCrawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.ErrorPage;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.SocketUtils;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.SortedSet;

@Component
@ConfigurationProperties("server")
public class ServerConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterCrawler.class);

    /*
    Added EmbeddedServletContainer as Tomcat currently. Need to change in future if  EmbeddedServletContainer get changed
 */
    private final int MIN_PORT = 1100;
    private final int MAX_PORT = 65535;
    /**
     * this is the read port from the applcation.yml file
     */
    @Value("${server.port}")
    private int port;

    @Value("${server.defaultport}")
    private int defaultPort;

    @Value("${server.hostname}")
    private String hostname;
    /**
     * this is the min port number that can be selected and is filled in from the application yml fil if it exists
     */
    private int maxPort = MIN_PORT;

    /**
     * this is the max port number that can be selected and is filled
     */
    private int minPort = MAX_PORT;

    /**
     * Added EmbeddedServletContainer as Tomcat currently. Need to change in future if  EmbeddedServletContainer get changed
     *
     * @return the container factory
     */
    @Bean
    public EmbeddedServletContainerFactory servletContainer() {
        return new TomcatEmbeddedServletContainerFactory();
    }

    @Bean
    public EmbeddedServletContainerCustomizer containerCustomizer() {
        return new EmbeddedServletContainerCustomizer() {
            @Override
            public void customize(ConfigurableEmbeddedServletContainer container) {
                // this only applies if someone has requested automatic port assignment
                if (port == 0) {
                    port = 0;
                    SortedSet<Integer> portSet = null;

                    int retry = 10;
                    do{
                        try {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(hostname, defaultPort), 5);
                            socket.close();
                            retry--;
                        } catch(Exception e){
                            port = defaultPort;
                        }
                    } while(port != defaultPort && retry > 0);

                    if (retry == 0){
                        LOGGER.warn("DEFAULT PORT '{}' ALRAEDY IN USE", defaultPort);
                        validatePorts();
                        port = SocketUtils.findAvailableTcpPort(minPort, maxPort);
                    }

                    container.setPort(port);
                }
                container.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/404"));
                container.addErrorPages(new ErrorPage(HttpStatus.FORBIDDEN, "/403"));
            }
        };
    }

    /**
     * validate the port choices
     * - the ports must be sensible numbers and within the alowable range and we fix them if not
     * - the max port must be greater than the min port and we set it if not
     */
    private void validatePorts() {
        if (minPort < MIN_PORT || minPort > MAX_PORT - 1) {
            minPort = MIN_PORT;
        }

        if (maxPort < MIN_PORT + 1 || maxPort > MAX_PORT) {
            maxPort = MAX_PORT;
        }

        if (minPort > maxPort) {
            maxPort = minPort + 1;
        }
    }
}
