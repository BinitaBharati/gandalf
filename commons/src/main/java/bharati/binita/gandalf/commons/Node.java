package bharati.binita.gandalf.commons;

/**
 * 
 * @author binita.bharati@gmail.com 
 * Representation of a LogAggregation node.
 *
 */
public class Node {

	private String category;
	private String ipAddress;
	private int port;

	public Node(String category, String ipAddress, int port) {
		this.category = category;
		this.ipAddress = ipAddress;
		this.port = port;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "CATEGORY = " + category + ", IP ADDRESS = " + ipAddress + "||";
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		Node input = (Node) obj;
		if (this.getCategory().equals(input.getCategory()) && this.getIpAddress().equals(input.getIpAddress())
				&& this.getPort() == input.getPort()) {
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 1;
	}

}
