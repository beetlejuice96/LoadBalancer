# LoadBalancer
this is a load balancer built for the course of Parallel &amp; distrbuted Programming



## Pasos a seguir.

1. ejecutar el archivo "deploy-automation.sh" que se encuentra en https://github.com/dpetrocelli/sdypp2020/tree/master/PI-UNLU/Clase8/rabbit de esta forma:

bash deploy-autiomation.sh

2. Definir los parametros de rabbit en `loadBalancer/src/main/java/resources/rabbitmq.properties` con sus datos.

3. Realizar la ejecucion en este orden.

- Dispatcher
- NodeMain
- ServerMain
- ClientGenerator

| Estado | Carga  | Acción
| :------------- | :------------- | :-------------
| **CRITICO**  | 80% - 100% | Se crean **nodosActivos/2** nuevos nodos (si es mayor a 1, sino 1)**
| **ALERT** | 50% - 80% | Se crean **nodosActivos/4** nuevos nodos (si es mayor a 1, sino 1)**
| **NORMAL** | 20% - 50% | -
| **IDLE** | 0% - 20% | Se eliminan **nodosActivos/3** nodos (si es mayor a 1, sino 1)
