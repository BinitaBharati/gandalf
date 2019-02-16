package bharati.binita.gandalf.loadbalancer.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bharati.binita.gandalf.commons.Node;

/**
 * 
 * @author binita.bharati@gmail.com 
 * 			Round-robin strategy implementation for the LoadBalancer service.
 *
 */

public class RoundRobinStrategy implements Strategy {

	private static Logger logger = LoggerFactory.getLogger(RoundRobinStrategy.class);

	private Properties prop;
	private Map<String, LinkedBlockingQueue<Node>> categoryToNodeList;
	// Important!! Do not update the below list once its constructed.
	private Map<String, List<Node>> categoryToNodeListImmutable;

	public RoundRobinStrategy(Properties prop) {
		this.prop = prop;
		this.categoryToNodeList = new ConcurrentHashMap<String, LinkedBlockingQueue<Node>>();
		this.categoryToNodeListImmutable = new HashMap<String, List<Node>>();

		String categories = prop.getProperty("lb.categories");
		String[] catList = categories.split(",");
		for (String eachCat : catList) {
			List<Node> tmp = initNodeList(eachCat);
			LinkedBlockingQueue<Node> nodeList = new LinkedBlockingQueue<Node>(tmp);
			categoryToNodeList.put(eachCat, nodeList);
			categoryToNodeListImmutable.put(eachCat, tmp);
		}

	}

	private List<Node> initNodeList(String category) {
		List<Node> retList = new ArrayList<Node>(Integer.parseInt(prop.getProperty("lb.category.max.nodes")));
		String nodeInfos = prop.getProperty("lb.category.node.service1.info." + category);
		String[] nodeInfoList = nodeInfos.split(",");
		for (String eachNodeInfo : nodeInfoList) {
			Node node = new Node(category, eachNodeInfo.substring(0, eachNodeInfo.indexOf(":")),
					Integer.parseInt(eachNodeInfo.substring(eachNodeInfo.indexOf(":") + 1)));
			retList.add(node);
		}
		return retList;

	}

	/**
	 * Below method is invoked by multiple threads concurrently
	 */
	public Node getMasterNode(String category) {
		String tName = Thread.currentThread().getName();
		Node masterNode = null;
		try {
			LinkedBlockingQueue<Node> nodeList = categoryToNodeList.get(category);
			masterNode = nodeList.take();
			logger.info(tName + " : getMasterNode --> got master node  = " + masterNode + "for category " + category);
			nodeList.add(masterNode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info(tName + " : getMasterNode --> existing with  = " + masterNode);
		return masterNode;
	}

	/**
	 * Below method is invoked by multiple threads concurrently
	 */
	public List<Node> getSlaveNodes(Node masterNode, String category) {
		String tName = Thread.currentThread().getName();
		logger.info(tName + " --> getSlaveNodes :  entered for " + category + " with masterNode  " + masterNode);
		List<Node> retList = new ArrayList<Node>();
		List<Node> nodeList = categoryToNodeListImmutable.get(category);
		// Filter out master node
		for (Node node : nodeList) {
			if (!node.equals(masterNode)) {
				retList.add(node);
			}
		}
		logger.info(tName + " --> getSlaveNodes :  nodeList for category " + category + " is " + retList);
		return retList;
	}

}
