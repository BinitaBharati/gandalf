package bharati.binita.gandalf.aggregator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.gandalf.commons.Constants;
import bharati.binita.gandalf.commons.SocketUtil;

/**
 * 
 * @author binita.bharati@gmail.com 
 * 		   This LogAggrgation service aggregate logs
 *         for various categories.Each category's log file is written into a
 *         <category> specific directory, under log.aggregator.main.dir defined
 *         in the property file.
 *
 */

public class LogAggregatorService {

	private static Logger logger = LoggerFactory.getLogger(LogAggregatorService.class);

	private static Properties prop;
	private static LogAggregatorService la;
	private ServerSocket socket;
	private ExecutorService threadPool;
	private LinkedBlockingQueue<Socket> connectedSocketsQ;
	private Callable<String> logMsgHandler;
	private AtomicBoolean acquireFileLock;

	/**
	 * Gets a singleton instance of LogAggregatorService
	 * 
	 * @param prop1
	 * @return
	 * @throws Exception
	 */
	public static LogAggregatorService getInstance(Properties prop1) throws Exception {
		if (la != null) {
			return la;
		}

		return new LogAggregatorService(prop1);
	}

	private LogAggregatorService(Properties prop) throws Exception {
		this.prop = prop;
		threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("log.aggregator.handler.tc")));
		String aggrSocketIps = prop.getProperty("log.aggregator.socket.ips");
		List<String> aggrSocketIpList = Arrays.asList(aggrSocketIps.split(","));
		String hostIp = null;
		InetAddress inetAddress = InetAddress.getLocalHost();
		hostIp = inetAddress.getHostAddress();
		logger.info("LogAggregatorService: hostIp = " + hostIp);
		if (aggrSocketIpList.contains(hostIp)) {
			socket = new ServerSocket(Integer.parseInt(prop.getProperty("log.aggregator.socket.port")),
					Integer.parseInt(prop.getProperty("log.aggregator.socket.backlog")), InetAddress.getByName(hostIp));
		}

		connectedSocketsQ = new LinkedBlockingQueue<Socket>(
				Integer.parseInt(prop.getProperty("log.aggregator.max.connected.sockets")));
		logMsgHandler = new Callable<String>() {

			public String call() throws Exception {
				// TODO Auto-generated method stub
				handleCase1();
				return null;
			}
		};
		acquireFileLock = new AtomicBoolean(false);

	}

	public void listen() {
		while (true) {
			/**
			 * Listen to incoming request in blocking mode.If multiple client request arrive
			 * concurrently, they should be stored in the OS backlog.I should not have to
			 * worry about concurrent calls to accept method atleast ?
			 */

			try {
				Socket newConnectedSocket = socket.accept();
				boolean status = connectedSocketsQ.add(newConnectedSocket);
				if (status) {
					threadPool.submit(logMsgHandler);
				} else {
					// May be queue has reached maximum capacity? What to do now ? Let client resend
					// it ?
					// Inform client about it.They may retry.
					logger.info("LogAggregatorService: listen --> Unable to service the input request");
					SocketUtil.sendMsg(newConnectedSocket, Constants.SERVER_ERROR
							+ "Unable to service the input request at the mopment. Please try again later.");

				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method will be invoked by multiple threads concurrently.
	 * 
	 * @throws Exception
	 */
	public void handleCase1() throws Exception {
		try {
			// Get head of queue in blocking mode
			Socket connectedSocket = connectedSocketsQ.take();
			DataInputStream in = new DataInputStream(connectedSocket.getInputStream());
			// Expecting input stream of format : CATEGORY=X;BODY=<entire log msg str ,
			// including the time stamp>
			String input = in.readUTF();
			logger.info("LogAggregrationService: handleCase1 : got msg = " + input + " from "
					+ connectedSocket.getRemoteSocketAddress());
			if (input.indexOf(Constants.MSG_CATEGORY + "=") == -1 || input.indexOf(Constants.MSG_BODY + "=") == -1) {
				// Need to inform client that he has sent invalid input stream.
				String errMsg = Constants.SERVER_ERROR
						+ "Please specify a valid log message format. Valid format looks like this : \n"
						+ "CATEGORY=<msg category>;BODY=<entire log msg string , including the time stamp>";
				SocketUtil.sendMsg(connectedSocket, errMsg);

			} else {
				String category = input.substring((Constants.MSG_CATEGORY + "=").length(), input.indexOf(";"));
				String msg = input.substring(input.indexOf(Constants.MSG_BODY) + (Constants.MSG_BODY + "=").length());
				writeToLog(category, msg);
				SocketUtil.sendMsg(connectedSocket, Constants.SERVER_SUCCESS);
				connectedSocket.close();
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeToLog(String category, String msg) throws Exception {
		String mainLogDir = prop.getProperty("log.aggregator.main.dir");// This directory would be pre-created
		File tmp = new File(mainLogDir + File.separator + category);
		while (true) {
			if (acquireFileLock.compareAndSet(false, true)) {
				if (!tmp.exists()) {
					boolean status = tmp.mkdirs();
				}
				File file = new File(tmp.getAbsolutePath() + File.separator + "service.log");
				PrintStream printStream = new PrintStream(new FileOutputStream(file, true));
				printStream.println(msg);
				printStream.close();
				acquireFileLock.set(false);
				break;
			}
			Thread.sleep(100);
		}

	}

}
