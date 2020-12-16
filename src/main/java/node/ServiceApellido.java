package node;

public class ServiceApellido implements Service {

    private int port;
    private String name;
    public ServiceApellido(int port, String name) {
        this.port = port;
        this.name = name;
    }



    public String getName() {
        return this.name;
    }

    public int getPort() {
        return this.port;
    }

    public Object execute(Object[] lista) {
        StringBuilder result = new StringBuilder("apellido:");
                //lista[i]+= "apellido";
                result.append(lista[0].toString());
        try {
            // Hacemos que la tarea tarde mas
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}
