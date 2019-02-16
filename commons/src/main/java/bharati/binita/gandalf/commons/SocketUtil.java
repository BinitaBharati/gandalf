package bharati.binita.gandalf.commons;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * 
 * @author binita.bharati@gmail.com
 *
 */

public class SocketUtil {
	
	public static void sendMsg(Socket socket, String msg) throws Exception {
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(msg);
		
	}

}
