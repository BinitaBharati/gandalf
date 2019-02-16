package bharati.binita.gandalf.aggregator.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.gandalf.commons.Constants;

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */


public class AggregatorClient {
	private static Logger logger = LoggerFactory.getLogger(AggregatorClient.class);

	
	private static Properties prop;
	private static ExecutorService threadPool;
	private Callable<String> callable;
	private String[] category;
	private String[] msgBody;
	Random junkRandom;
	private String[] junkMsg;
	
	public AggregatorClient() {
		init();
	}
	
	public void init() {
		prop = new Properties();

		try {
			// Load property file
			InputStream input = AggregatorClient.class.getClassLoader().getResourceAsStream("client.properties");
			prop.load(input);
			threadPool = Executors.newFixedThreadPool(200);
			category = new String[]{"inventory", "fault", "provision"};
			msgBody = new String[]{"blah blah howdy","Once upon a time there lived a queen","The case of the sour grapes","Triceratops anyone?",
					"This is just a plain big sentence of text. I dont know what to write, but, neverthless, I write!"};
			junkRandom = new Random();
			junkMsg = new String[] {"crap crap crap"};
			callable = new Callable<String>() {

				public String call() throws Exception {
					String randomMsg =  null;
					//Generate random message
					int random0or1 = junkRandom.nextInt(2);
					if (random0or1 == 0) {
						//Send valid msg - does not matter if Random.nextInt si not thread safe. You just need some random number.
						String categoryVal = category[junkRandom.nextInt(category.length)];
						String msgBodyVal = msgBody[junkRandom.nextInt(msgBody.length)];
						randomMsg = Constants.MSG_CATEGORY+"="+categoryVal+";"+Constants.MSG_BODY+"="+msgBodyVal;
						sendLogForAggregation(randomMsg);
					} else {
						//Send junk message
						randomMsg = junkMsg[0];
						sendLogForAggregation(junkMsg[0]);
					}
					return randomMsg;
				}
			};
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void test() {
		int validMsgCount = 0; int invalidMsgCount = 0;
		for (int i = 0 ; i < 200 ; i++) {
			try {
				Future<String> randomMsg = threadPool.submit(callable);
				if (randomMsg.get().startsWith(Constants.MSG_CATEGORY)) {
					validMsgCount++;
				} else {
					invalidMsgCount++;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void sendLogForAggregation(String input) throws Exception {
		String tName = Thread.currentThread().getName();
		System.out.println(tName + " --> Sending input = "+input);
		Socket client = new Socket(prop.getProperty("log.aggregator.socket.ip"), Integer.parseInt(prop.getProperty("log.aggregator.socket.port")));        
        //System.out.println("LogAggregatorClient: Just connected to " + client.getRemoteSocketAddress());
        OutputStream outToServer = client.getOutputStream();
        DataOutputStream out = new DataOutputStream(outToServer);
        
        StringBuffer sb = new StringBuffer();
        
        //out.writeUTF(Constants.MSG_CATEGORY + "=" + category + ";" + Constants.MSG_BODY + "="+logbody);
        out.writeUTF(input);
        
        InputStream inFromServer = client.getInputStream();
        DataInputStream in = new DataInputStream(inFromServer); 
        System.out.println(tName + " --> about to read utf " );
        System.out.println(tName + " --> Server says " + in.readUTF());
        client.close();
	}
	
	public static void main(String[] args) {
		AggregatorClient agrClient = new AggregatorClient();
		agrClient.test();
	}

}
