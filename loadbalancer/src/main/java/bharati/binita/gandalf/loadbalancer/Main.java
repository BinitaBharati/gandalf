package bharati.binita.gandalf.loadbalancer;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author binita.bharati@gmail.com 
 * Main class that starts the LoadBalancer service.
 *
 */

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		logger.info("Main: entered");
		LoadBalancerService instance = LoadBalancerService.getInstance();
		instance.listen();

	}
}
