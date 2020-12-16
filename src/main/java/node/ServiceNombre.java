package node;

import java.util.Arrays;

public class ServiceNombre implements Service {

    private int port;
    private String name;
    public ServiceNombre(int port, String name) {
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
        StringBuilder result = new StringBuilder("nombre:");
        for (int i=0;i<lista.length;i++) {
            if (i!=0) {
                //lista[i]+= "nombre";
                result.append(lista[i].toString());}
        }
        try {
            // Hacemos que la tarea tarde mas
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}
