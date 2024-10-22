package server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import core.Message;

public class ServerMain {
	private final Logger log = LoggerFactory.getLogger(ServerMain.class);
	public static final String RABBITMQ_CONFIG_FILE="src/main/java/resources/rabbitmq.properties";
	private String inputQueueName;
	private String outputQueueName;
	private String username;
	private String password;
	private ConnectionFactory connectionFactory;
	private Connection queueConnection;
	private Channel queueChannel;
	private String ip;
	private int port;
	private String ipRabbit;
	private int portRabbit;
	private ServerSocket ss;
	private HashMap<String, Message> results;

	public ServerMain(String ip, int port) {
		configRabbitParms();
		this.port=port;
		this.ip=ip;
		this.inputQueueName = "inputQueue";
		this.outputQueueName = "outputQueue";
		this.configureConnectionToRabbit();
		log.info(" SE HA ESTABLECIDO LA CONEXION CON RABBIT");
		this.results = new HashMap<>();

	}

	void configRabbitParms() {
		try (InputStream input = new FileInputStream(RABBITMQ_CONFIG_FILE)) {
			Properties propiedades = new Properties();
			// leo las propiedades de un archivo externo que tiene ip por user y pass
			propiedades.load(input);
			this.ipRabbit = propiedades.getProperty("IP");
			this.portRabbit = Integer.parseInt(propiedades.getProperty("PORT"));
			this.username = propiedades.getProperty("USER");
			this.password = propiedades.getProperty("PASS");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}


	private void configureConnectionToRabbit() {
		try {
			this.connectionFactory = new ConnectionFactory();
			this.connectionFactory.setHost(this.ipRabbit);
			this.connectionFactory.setPort(this.portRabbit);
			this.connectionFactory.setUsername(this.username);
			this.connectionFactory.setPassword(this.password);
			this.queueConnection = this.connectionFactory.newConnection();
			this.queueChannel = this.queueConnection.createChannel();
			//declaro cola entrada
			this.queueChannel.queueDeclare(this.inputQueueName, true, false, false, null);
		} catch (ConnectException e) {
			System.err.println("[!] RabbitMQ Server is down.");
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		int nroThread = (int) Thread.currentThread().getId();
		String packetName = ServerMain.class.getSimpleName()+"-"+nroThread;
		System.setProperty("log.name",packetName);
		ServerMain sm = new ServerMain("localhost", 30000);
		sm.startServer();
	}

	public void startServer() {
		try {
			ServerSocket ss = new ServerSocket (this.port);
			log.info("Server started on " + this.port);
			Random r = new Random();
			while (true) {
				Socket client = ss.accept();
				log.info("Cliente conectado de " + client.getInetAddress().getCanonicalHostName()+":"+client.getPort());
				// Genera un ID Random de para el Thread -> TODO: Reemplazar por funcion mas potente.
				long routingKey =  r.nextLong();
				if (routingKey < 0) routingKey *= -1;
				ThreadServer ts = new ThreadServer(client, routingKey, this.queueChannel, this.inputQueueName, log, results);
				log.info("Nuevo ThreadServer: "+ routingKey);
				Thread tsThread = new Thread(ts);
				tsThread.start();
			}
		} catch (IOException e) {
			log.info("Port in use!");
		}
	}

}
