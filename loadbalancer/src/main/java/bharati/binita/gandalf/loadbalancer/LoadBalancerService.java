package bharati.binita.gandalf.loadbalancer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.gandalf.commons.Constants;
import bharati.binita.gandalf.commons.Node;
import bharati.binita.gandalf.commons.SocketUtil;
import bharati.binita.gandalf.loadbalancer.strategy.RoundRobinStrategy;
import bharati.binita.gandalf.loadbalancer.strategy.Strategy;

/**
 * 
 * @author binita.bharati@gmail.com 
 * 		   Mediator to the gandalf log aggregator
 *         service. This load balances the request to the underlying aggregator
 *         service(s). It decides to send the log to the target aggregator
 *         service based on the CATEGORY field present in the log message
 *         itself. Also, this service is the single point of failure to access
 *         the log aggregation service.
 *
 *
 */

public class LoadBalancerService {

	private static Logger logger = LoggerFactory.getLogger(LoadBalancerService.class);

	private Properties prop;
	private ServerSocket socket;
	private LinkedBlockingQueue<Socket> connectedSocketsQ;
	ExecutorService threadPool;
	private Callable<String> logRequestHandler;
	private Strategy strategy;
	private static LoadBalancerService instance;

	public static LoadBalancerService getInstance() {
		if (instance == null) {
			instance = new LoadBalancerService();
		}
		return instance;

	}

	private LoadBalancerService() {
		prop = new Properties();
		try {
			// Load property file
			InputStream input = LoadBalancerService.class.getClassLoader().getResourceAsStream("lb.properties");
			prop.load(input);
			String lbStrategy = prop.getProperty("lb.strategy");

			connectedSocketsQ = new LinkedBlockingQueue<Socket>(
					Integer.parseInt(prop.getProperty("lb.service.queue.size")));
			threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("lb.service.thread.count")));
			logRequestHandler = new Callable<String>() {

				public String call() throws Exception {
					handleLogRequest();
					return null;
				}
			};
			if (lbStrategy.equals("roundrobin")) {
				strategy = new RoundRobinStrategy(prop);
			}
			socket = new ServerSocket(Integer.parseInt(prop.getProperty("lb.service.port")),
					Integer.parseInt(prop.getProperty("lb.service.backlog")),
					InetAddress.getByName(prop.getProperty("lb.service.ip")));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void listen() {
		while (true) {
			try {

				Socket connectedSocket = socket.accept();
				boolean status = connectedSocketsQ.add(connectedSocket);
				if (status) {
					threadPool.submit(logRequestHandler);
				} else {
					logger.info("LoadBalancer: listen --> Unable to service the input request");
					SocketUtil.sendMsg(connectedSocket, Constants.SERVER_ERROR
							+ "Unable to service the input request at the mopment. Please try again later.");
					connectedSocket.close();
				}
				Thread.sleep(100);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	private void handleLogRequest() throws Exception {
		String tName = Thread.currentThread().getName();
		try {
			// Get head of queue in blocking mode
			Socket connectedSocket = connectedSocketsQ.take();
			DataInputStream in = new DataInputStream(connectedSocket.getInputStream());
			// Expecting input stream of format : CATEGORY=X;BODY=<entire log msg str ,
			// including the time stamp>
			String input = in.readUTF();
			logger.info(tName + ": LoadBalancer: handleLogRequest --> got msg = " + input + " from "
					+ connectedSocket.getRemoteSocketAddress());
			if (input.indexOf(Constants.MSG_CATEGORY + "=") == -1 || input.indexOf(Constants.MSG_BODY + "=") == -1) {
				// Need to inform client that he has sent invalid input stream.
				String errMsg = Constants.SERVER_ERROR
						+ "Please specify a valid log message format. Valid format looks like this : \n"
						+ "CATEGORY=<msg category>;BODY=<entire log msg string , including the time stamp>";
				SocketUtil.sendMsg(connectedSocket, errMsg);
				connectedSocket.close();

			} else {
				String category = input.substring((Constants.MSG_CATEGORY + "=").length(), input.indexOf(";"));
				Node masterNode = strategy.getMasterNode(category);
				logger.info(tName + " masterNode = " + masterNode);
				String response = invokeAggrService(masterNode, input);
				logger.info(tName + " abt to get slaves nodes for = " + masterNode);
				List<Node> slaveNodes = strategy.getSlaveNodes(masterNode, category);
				logger.info(tName + " slaveNodes = " + slaveNodes);

				for (Node slave : slaveNodes) {
					invokeAggrService(slave, input);
				}
				SocketUtil.sendMsg(connectedSocket, response);
				connectedSocket.close();
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String invokeAggrService(Node curNode, String input) throws Exception {
		String tName = Thread.currentThread().getName();
		Socket nodeSocket = new Socket(curNode.getIpAddress(), curNode.getPort());
		SocketUtil.sendMsg(nodeSocket, input);
		InputStream inFromNodeServer = nodeSocket.getInputStream();
		DataInputStream nodeSocketIs = new DataInputStream(inFromNodeServer);
		String retData = nodeSocketIs.readUTF();
		nodeSocket.close();
		logger.info(tName + ": LoadBalancer: invokeAggrService --> Node Server says " + retData);
		return retData;
	}

}
