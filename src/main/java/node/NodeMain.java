package node;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import core.Message;
import server.ServerMain;


public class NodeMain {
	private static final String EXCHANGE_OUTPUT = "XCHNG-OUT";
	private final Logger log = LoggerFactory.getLogger(NodeMain.class);
	private String username;
	private String password;
	private ConnectionFactory connectionFactory;
	private Connection queueConnection;
	private Channel queueChannel;
	private String outputQueueName;
	private String activesQueueName;
	private String myNodeQueueName;
	private String notificationQueueName;
	public static final String RABBITMQ_CONFIG_FILE="src/main/java/resources/rabbitmq.properties";

	private String ipRabbitMQ;
	private int portRabbitMQ;
	private Node node;
	private Gson googleJson;
	private int max_tasks;
	private String ipServer;
	private int portServer;

	private static final ArrayList<String> DICCIONARIO = new ArrayList<String>(Arrays.asList(
			"NodoA", "NodoB", "NodoC", "NodoD", "NodoE", "NodoF", "NodoG", "NodoH",
			"NodoI", "NodoJ", "NodoK", "NodoL", "NodoM", "NodoN", "NodoO", "NodoP",
			"NodoQ", "NodoR", "NodoS", "NodoT", "NodoU", "NodoV", "NodoW", "NodoX",
			"NodoY", "NodoZ"
	));

	public Node getNode() {
		return this.node;
	}

	public NodeMain(Node node, String ipServerResult, int portServerResult) {
		this.node = node;
		configRabbitParms();
		this.ipServer = ipServerResult;
		this.portServer = portServerResult;
		this.outputQueueName = "outputQueue";
		this.notificationQueueName = "notificationQueue";
		this.myNodeQueueName = this.node.getName();
		googleJson = new Gson();
		this.configureConnectionToRabbit();
		log.info(" CONEXION ESTABLECIDA CON RABBIT ");
	}

	void configRabbitParms() {
		try (InputStream input = new FileInputStream(RABBITMQ_CONFIG_FILE)) {
            Properties propiedades = new Properties();
            // CARGO LAS PROPIEDADES DE UN ARCHIVO DE CONFIG
            propiedades.load(input);
            this.ipRabbitMQ = propiedades.getProperty("IP");
            this.portRabbitMQ = Integer.parseInt(propiedades.getProperty("PORT"));
            this.username = propiedades.getProperty("USER");
            this.password = propiedades.getProperty("PASS");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
	}

	private void configureConnectionToRabbit() {
		try {
			this.connectionFactory = new ConnectionFactory();
			this.connectionFactory.setHost(this.ipRabbitMQ);
			this.connectionFactory.setPort(this.portRabbitMQ);
			this.connectionFactory.setUsername(this.username);
			this.connectionFactory.setPassword(this.password);
			this.queueConnection = this.connectionFactory.newConnection();
			this.queueChannel = this.queueConnection.createChannel();
			this.queueChannel.queueDeclare(this.outputQueueName, true, false, false, null);
			this.queueChannel.queueDeclare(this.myNodeQueueName, true, false, false, null);
		} catch (ConnectException e) {
			System.err.println("[!] EL SERVIDOR DE RABBIT ESTA INACTIVO.");
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		int nroThread = (int) Thread.currentThread().getId();
		String packetName = ServerMain.class.getSimpleName()+"-"+nroThread;
		System.setProperty("log.name",packetName);

		ArrayList<NodeMain> nodo = new ArrayList<NodeMain>();
		int aux=0;
		for( String Nodo : DICCIONARIO){
			nodo.add(new NodeMain(new Node(Nodo, "localhost",8071,20), "localhost",30000 ));
			nodo.get(aux).node.addService(new ServiceNombre(8071,"nombre"));
			nodo.get(aux).node.addService(new ServiceApellido(8071,"apellido"));
			nodo.get(aux).startNode();
			aux++;

		}
	}


	public void startNode() {
		try {
			log.info(this.node.getName()+" Started");
			DeliverCallback deliverCallback = (consumerTag, delivery) -> {

				Message message = googleJson.fromJson(new String(delivery.getBody(), "UTF-8"), Message.class);
				log.info("["+ delivery.getEnvelope().getRoutingKey() + "] received: " + googleJson.toJson(message));
				//Asigna Tarea a Thread
				log.info("["+this.node.getName()+"] " + "Working...");

				//asigno la tarea a un thread
				Random r = new Random();
				ThreadNode tn = new ThreadNode(r.nextLong(), this.node,Long.parseLong(message.getHeader("token-id")),message, queueChannel,this.activesQueueName, outputQueueName,log, ipServer, portServer);
				Thread nodeThread = new Thread(tn);
				nodeThread.start();
			};
			queueChannel.basicConsume(this.myNodeQueueName, true, deliverCallback, consumerTag -> {});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}




}
