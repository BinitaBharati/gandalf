package bharati.binita.gandalf.loadbalancer.strategy;


import java.util.List;

import bharati.binita.gandalf.commons.Node;

/**
 * 
 * @author binita.bharati@gmail.com
 * The load balancer strategy.
 *
 */

public interface Strategy {
	
	public Node getMasterNode(String category);
	
	public List<Node> getSlaveNodes(Node masterNode, String category);

}
