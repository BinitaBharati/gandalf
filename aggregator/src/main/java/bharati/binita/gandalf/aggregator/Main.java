package bharati.binita.gandalf.aggregator;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author binita.bharati@gmail.com 
 * 		   Main class to launch the LogAggregation service.
 *
 */

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
		logger.info("Main: entered");
		// Init property
		Properties prop = new Properties();

		try {
			// Load property file
			InputStream input = Main.class.getClassLoader().getResourceAsStream("aggr.properties");
			prop.load(input);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Init Aggr service listener
		LogAggregatorService nl = LogAggregatorService.getInstance(prop);
		nl.listen();

	}

}
