# Sistemas Distribuidos I

Este repositorio contiene el trabajo práctico realizado para la materia Sistemas Distribuidos I de la Facultad de Ingeniería de la Universidad de Buenos Aires.

## Integrantes

- [Alan Botoshansky](https://github.com/AlanBotoshansky) - Padrón: 108933
- [Ivan Botoshansky](https://github.com/IvanBotoshansky) - Padrón: 108897
- [Bruno Pascaretta](https://github.com/BrunoPasca) - Padrón: 107840

---

## Diseño

### Vista Física

#### Diagrama de Robustez

![Diagrama de Robustez](./diagramas/robustez/diagrama_robustez.png)

En el diagrama de robustez podemos observar un funcionamiento general del sistema. Se puede ver la manera en la que se ingresan los datos al sistema, y como colaboran y se comunican (mediante colas) los distintos controllers para resolver la consultas, hasta entregar los resultados de las distintas queries al cliente.

Los distintos controllers que podemos identificar en el diagrama se encargan de realizar distintas tareas:

- **Data Cleaner**: Elimina campos innecesarios y distribuye la información para la resolución de las queries.
- **Filters**: Filtran la información según distintos criterios.
- **Aggregators**: Realizan cálculos sobre los datos recibidos (como pueden ser sumas, promedios, entre otros).
- **Routers**: Redirigen la información hacia distintas colas según el valor de un campo determinado.
- **Joiners**: Juntan información de distintas fuentes según el valor de un campo determinado.
- **Sentiment Analyzer**: Es el encargado de realizar el análisis de sentimiento del overview de las películas para la query 5.
- **Results Handler**: Recibe los resultados y se los envía al cliente.

#### Diagrama de Despliegue

![Diagrama de Despliegue](./diagramas/despliegue/diagrama_despliegue.png)

En el diagrama de despliegue podemos observar la topologia del sistema. El nodo central es el que aloja el servicio de RabbitMQ, y los demás nodos son los que alojan los distintos servicios para resolver las consultas. Cada uno de estos nodos se encarga de realizar una tarea específica, y se comunican entre sí mediante colas de RabbitMQ. El cliente se conecta al nodo central, y éste se encarga de redirigir la información a los distintos nodos para resolver las consultas.

### Vista de Procesos

#### Diagramas de Actividades

En esta sección se pueden observar los distintos diagramas de actividades para cada una de las queries. En cada uno de ellos se puede ver el flujo de trabajo y las distintas tareas que se realizan para resolver la consulta. Existe un diagrama de actividades para cada una de las queries ya que estas son independientes entre sí, por lo que se pueden realizar en paralelo, y cada una de ellas tiene un flujo de trabajo diferente.

##### Diagrama de Actividades de la Query 1

![Diagrama de Actividades 1](./diagramas/actividades/q1_diagrama_actividades.png)

##### Diagrama de Actividades de la Query 2

![Diagrama de Actividades 2](./diagramas/actividades/q2_diagrama_actividades.png)

##### Diagrama de Actividades de la Query 3

![Diagrama de Actividades 3](./diagramas/actividades/q3_diagrama_actividades.png)

##### Diagrama de Actividades de la Query 4

![Diagrama de Actividades 4](./diagramas/actividades/q4_diagrama_actividades.png)

##### Diagrama de Actividades de la Query 5

![Diagrama de Actividades 5](./diagramas/actividades/q5_diagrama_actividades.png)

#### Diagramas de Secuencia

En esta sección se pueden observar los distintos diagramas de secuencia para cada una de las queries. En cada uno de ellos se puede ver de forma cronológica el intercambio de mensajes entre los distintos actores del sistema para resolver la consulta en cuestión.

##### Diagrama de Secuencia de la Query 1

![Diagrama de secuencia 1](./diagramas/secuencia/q1_diagrama_secuencia.png)

##### Diagrama de Secuencia de la Query 2

![Diagrama de secuencia 2](./diagramas/secuencia/q2_diagrama_secuencia.png)

##### Diagrama de Secuencia de la Query 3

![Diagrama de secuencia 3](./diagramas/secuencia/q3_diagrama_secuencia.png)

##### Diagrama de Secuencia de la Query 4

![Diagrama de secuencia 4](./diagramas/secuencia/q4_diagrama_secuencia.png)

##### Diagrama de Secuencia de la Query 5

![Diagrama de secuencia 5](./diagramas/secuencia/q5_diagrama_secuencia.png)

### Vista de Desarrollo

#### Diagrama de Paquetes

![Diagrama de Paquetes](./diagramas/paquetes/diagrama_paquetes.png)

En el diagrama de paquetes podemos observar la estructura del proyecto. Cada uno de los paquetes representa un módulo del sistema, y cada uno de ellos tiene una funcionalidad específica. Se pueden ver las dependencias entre los distintos paquetes, y en algunos de ellos se detallan los distintos componentes que incluyen. Por ejemplo, en el paquete `messages` se pueden ver los distintos mensajes que se utilizan para la comunicación entre los distintos nodos del sistema.

### DAG

![DAG](./diagramas/dag/dag.png)

El DAG (Direct Acyclic Graph) es una representación gráfica de las distintas tareas que se realizan para resolver las consultas. Cada nodo del DAG representa una tarea, y cada arista representa una dependencia entre las distintas tareas. El DAG permite visualizar de forma clara el flujo de trabajo del sistema, y cómo se distribuyen las tareas entre los distintos nodos. Además, en las aristas se especifican los distintos campos de la información que fluye entre los nodos. También se puede ver que a medida que fluye la información, hay campos que se van eliminando, ya que no son necesarios, y otros que se van agregando a medida que se van realizando las distintas tareas.

---

## Múltiples Clientes

Para que el sistema pueda soportar múltiples clientes, se realizaron las siguientes modificaciones:

### Campo `client_id` en todos los paquetes

Se agregó un campo `client_id` en todos los paquetes que se envían entre los distintos nodos del sistema. Este campo permite identificar a qué cliente pertenece cada paquete, y así poder enviarle la respuesta correspondiente. De esta manera, los clientes pueden realizar consultas simultáneamente, y el sistema puede responderle a cada uno de ellos de forma independiente. Gracias al `client_id`, los controllers que mantienen estado, como lo son los `Joiners` y `Aggregators`, pueden mantener el estado de cada cliente de forma independiente. Luego, el `Results Handler` envía los resultados correspondientes a cada cliente de acuerdo al `client_id` de cada resultado.

### Asignación de id único a cada cliente

Cada cliente, al conectarse al `Data Cleaner`, recibe un id único que lo identifica. Luego, envía este id al `Results Handler` para que éste luego pueda enviarle los resultados que le correspondan.

El siguiente diagrama ilustra la forma en la que se asigna el id único a cada cliente:

![Asignacion de id](./diagramas/asignacion_id/diagrama_asignacion_id.png)

### Limpieza de estado de los clientes en los controllers

Se implementó un mecanismo de limpieza de estado en los controllers que mantienen estado, como lo son los `Joiners` y `Aggregators`.

Por un lado, luego de haber procesado la totalidad de los paquetes de un cliente, se eliminan los datos de estado de ese cliente. Por ejemplo, en los `Joiners`, luego de haber unido todos los paquetes de un cliente, se eliminan las péliculas que se tenían guardadas en memoria de ese cliente. De esta manera, se libera memoria y se evita que el sistema se sature con datos innecesarios.

Por el otro lado, si un cliente se desconecta de forma abrupta, se envia desde el `Data Cleaner` y se propaga a través de los distintos controllers un mensaje de tipo `ClientDisconnected`, que indica que el cliente se ha desconectado. Este mensaje es recibido por los `Joiners` y `Aggregators`, y se eliminan los datos de estado de ese cliente. De esta manera, se evita que el sistema mantenga datos innecesarios de clientes que ya no están conectados.

### Limite de cantidad de clientes que realizan consultas simultáneamente

Se implementó un mecanismo para limitar la cantidad de clientes que pueden realizar consultas simultáneamente. Dicha cantidad puede ser configurada en el archivo `config.ini` del `Data Cleaner` o pasado como variable de entorno llamada `MAX_CONCURRENT_CLIENTS`. Esto lo hicimos para evitar que el sistema reserve recursos de forma ilimitada, y así evitar ataques de denegación de servicio (DoS). Además, esto permite que el sistema atienda a los clientes de a "turnos", y así evitar que el sistema se sature, tanto en memoria como en CPU. Si se quieren atender a más clientes, se puede aumentar el límite de clientes simultáneos, y a su vez escalar el sistema horizontalmente, agregando más nodos al sistema.
