package client;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.slf4j.Logger;
import com.google.gson.Gson;
import core.Message;

public class ClientTCP implements Runnable {
	Socket socket;
	int numClient;
	static int REQUESTS = 30;
	private final Logger log;
	String[] nombres={"Juan", "José", "Miguel", "Antonio"};
	String[] apellidos={"fernandez", "galvez", "Mendez", "Artuan"};

	public ClientTCP (String ip, int port, Logger log) throws IOException {
		this.socket = new Socket(ip,port);
		this.log=log;
		this.numClient= (int) (Math.random()* 100)+1;
	}

	@Override
	public void run() {
		int pos;
		log.info("-x-x-x- CLIENT "+numClient+" STARTED -X-X-X-X-");
		try {
			ObjectOutputStream canalSalida = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream canalEntrada = new ObjectInputStream(socket.getInputStream());
			int contador = REQUESTS;
			while (contador-- > 0){
				String call = (Math.random() * 10 > 5) ? "nombre" : "apellido";
				Message function = new Message(call);
				pos = (int) (Math.random() * 3);
				function.addParametro("nombre", nombres[pos]);
				function.addParametro("apellido", apellidos[pos]);
				function.setHeader("client",String.valueOf(numClient));
				function.setHeader("tipo","query");
				//envio al server
				canalSalida.writeObject(function);
				log.info("["+numClient+"] ("+contador+") [C->S] Mensaje enviado de cliente a server " + (new Gson()).toJson(function));
				//leer del servidor
				Message respuesta = (Message) canalEntrada.readObject();
				log.info("["+numClient+"] ("+contador+")[S->C] El servidor ha respondido al cliente "+respuesta.getResultado());
			}
			socket.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
