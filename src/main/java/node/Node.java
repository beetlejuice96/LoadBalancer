package node;

import java.util.ArrayList;

public class Node {
	ArrayList<Service> servicios;
	private String name;
	private String ip;
	private int port;
	private int cargaMax;
	private int cargaActual;
	private float porcentajeCarga;
	private NodeState state;

	public Node(String name, String ip, int port, int maxLoad) {
		this.name = name;
		this.ip = ip;
		this.port = port;
		servicios = new ArrayList<>();
		this.cargaMax = maxLoad;
		this.cargaActual = 0;
		this.porcentajeCarga = 0;
		this.state = NodeState.IDLE;
	}
	
	
	public int getPort() {
		return this.port;
	}
	public int getCargaMax() {
		return cargaMax;
	}

	public void setCargaMax(int cargaMax) {
		this.cargaMax = cargaMax;
	}

	public int getCargaActual() {
		return cargaActual;
	}

	public void increaseCurrentLoad(int currentLoad) {
		this.cargaActual += currentLoad;
		this.updatePercentageLoad();
		this.updateState();
	}

	public void decreaseCurrentLoad(int currentLoad) {
		if (getCargaActual() > 0) {
			this.cargaActual -= currentLoad;
			this.updatePercentageLoad();
			this.updateState();
		}
	}

	private void updateState() {
		if (getPercentageLoad() == 0) {
			this.setState(NodeState.IDLE);
		} else if (getPercentageLoad() < 0.60) {
			this.setState(NodeState.NORMAL);
		} else if (getPercentageLoad() < 0.80) {
			this.setState(NodeState.ALERT);
		} else {
			this.setState(NodeState.CRITICAL);
		}
	}

	public NodeState getState() {
		return state;
	}

	public void setState(NodeState state) {
		this.state = state;
	}

	public String getName() {
		return name;
	}

	public void addService(Service service) {
		this.servicios.add(service);
	}

	public void delService(Service service) {
		this.servicios.remove(service);
	}

	public ArrayList<Service> getServicios() {
		return this.servicios;
	}

	public float getPercentageLoad() {
		return porcentajeCarga;
	}

	private void updatePercentageLoad() {
		this.porcentajeCarga = getCargaActual()/ getCargaMax();
	}
	
	public boolean hasService(String name){
		boolean find = false;
		for (Service s : servicios) {
			if (s.getName().equals(name)) {
				find = true;
				break;
			}
		}
		return find;
	}

	public Service findServiceByName(String name) {
		Service find = null;
		int i = 0;
		boolean salir = false;
		while (!salir && i<this.getServicios().size()){
			if (this.servicios.get(i).getName().equals(name)) {
					find = this.servicios.get(i);
					salir = true;
			}
			i++;
		}
		System.out.println(" {FIND} " + find + " - " + name);
		return find;
	}


	@Override
	public boolean equals(Object node) {
		if(node instanceof Node)
			return ((Node) node).getName().equals(this.getName());
		return false;
	}

	public void increaseCurrentLoad() {
		this.increaseCurrentLoad(1);
	}

	public void decreaseCurrentLoad() {
		this.decreaseCurrentLoad(1);
	}
}
