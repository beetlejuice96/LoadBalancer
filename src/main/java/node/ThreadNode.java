package node;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import core.Message;

public class ThreadNode implements Runnable {

	private Channel queueChannel;
	private String activeQueueName;
	private String outputQueueName;
	private String routingKey;
	private Gson googleJson;
	private Logger log;
	private Message tarea;
	private Node node;
	private Long id;
	private Socket socketServer;

	private static final String EXCHANGE_OUTPUT = "XCHNG-OUT";

	public ThreadNode(Long idThread,Node node, Long routingKey, Message message, Channel queueChannel, String activeQueueName, String outputQueueName, Logger log, String ipServerResult, int portServerResult) {
		this.id = idThread;
		this.node = node;
		this.routingKey = String.valueOf(routingKey);
		this.queueChannel = queueChannel;
		this.activeQueueName = activeQueueName;
		this.outputQueueName = outputQueueName;
		this.tarea = message;
		this.googleJson = new Gson();
		this.log = log;
		try {
			this.socketServer = new Socket(ipServerResult, portServerResult);
		} catch (ConnectException e) {
			System.err.println("[!] Server is down.");
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
		try {
			ObjectOutputStream canalSalida = new ObjectOutputStream(this.socketServer.getOutputStream());

			Service s = this.node.findServiceByName(this.tarea.getFunctionName());
			if (s != null){
				//seteo respuesta y la envio
				log.info("TASK - "+ s.getName());
				log.info("["+ this.node.getName()+ " - Thread "+this.id+"] :" +this.tarea.parametros.values());
				this.tarea.setResultado(s.execute(this.tarea.parametros.values().toArray()));
				log.info("["+ this.node.getName()+ " - Thread "+this.id+"] RESULTADO TAREA: "+ this.tarea.getResultado());

				Message respuesta = this.tarea;
				respuesta.setHeader("tipo","response");
				//envia a traves de cola salida
				String msgString = googleJson.toJson(respuesta);
				queueChannel.basicPublish(EXCHANGE_OUTPUT,"", MessageProperties.PERSISTENT_TEXT_PLAIN, msgString.getBytes(StandardCharsets.UTF_8));
				canalSalida.writeObject(respuesta);
				log.info("["+ this.node.getName()+ " - Thread "+this.id+"]  HA ENVIADO LA RESPUESTA.... "+ googleJson.toJson(respuesta));

			}

		} catch (IOException e) {
			e.printStackTrace();

		}
	}
}
