package client;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import server.ServerMain;

public class ClientGenerator {
	private final static Logger log = LoggerFactory.getLogger(ClientGenerator.class);

	public static void main(String[] args) throws IOException {
		int contador = 100;
		int nroHilo = (int) Thread.currentThread().getId();
		String nombre = ServerMain.class.getSimpleName()+"-"+nroHilo;
		System.setProperty("log.name",nombre);
		ClientTCP cliente;
		while(contador>0){
			cliente = new ClientTCP("localhost", 30000, log);
			Thread hilo = new Thread(cliente);
			hilo.start();
			contador--;
		}
	}
}
