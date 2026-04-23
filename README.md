# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.


# Informe


## Coordinación entre instancias de Sum

Cada instancia de Sum recibe una porción de los mensajes de datos gracias al mecanismo de distribución round-robin de RabbitMQ sobre la `INPUT_QUEUE` compartida. Cada instancia acumula localmente los totales por fruta para cada `client_id`.

Ante el problema de la notificación del EOF: el Gateway envía un único mensaje EOF a la `INPUT_QUEUE`, que lo recibe una sola instancia de Sum. Para que todas las instancias procesen su propio EOF, las instancias de Sum se comunican entre sí mediante colas de control dedicadas.

Cada instancia `i` declara y consume una cola propia llamada `SUM_CONTROL_QUEUE_i`. Estas colas son distintas de la `INPUT_QUEUE` de datos — no las usa el Gateway, sino exclusivamente las otras instancias de Sum para notificarse entre sí. Cada instancia suscribe su cola de control en el mismo canal de RabbitMQ que su cola de datos, de modo que ambas se consumen en el mismo event loop sin necesidad de threads adicionales.

Cuando la instancia `X` recibe el EOF del Gateway en su `INPUT_QUEUE`, el flujo es el siguiente: primero procesa su propio EOF inmediatamente — calcula sus totales acumulados y los envía al Aggregation — y luego publica un mensaje EOF de control en las colas `SUM_CONTROL_QUEUE_i` de todas las demás instancias (`i ≠ X`). Cada una de esas instancias, al recibir el mensaje de control en su event loop, procesa su propio EOF de la misma forma.

El hecho de que la instancia `X` se procese a sí misma directamente — y no a través de su propia cola de control — es clave para evitar una race condition: si `X` publicara en su propia cola de control, el mensaje podría procesarse antes de que `X` terminara de consumir todos los mensajes de datos pendientes en su `INPUT_QUEUE`. Al procesar el EOF directamente y solo notificar a las demás, se garantiza el orden correcto en todos los casos.

Para distribuir el trabajo hacia Aggregation sin redundancia, cada fruta se enruta a un único Aggregator determinado por `sum(fruta.encode()) % AGGREGATION_AMOUNT`. El EOF, en cambio, se envía en broadcast a todos los Aggregators ya que cada uno necesita contarlo.

## Coordinación entre instancias de Aggregation

Cada instancia de Aggregation recibe datos de un subconjunto disjunto de frutas — aquellas cuyo hash corresponde a su ID — y acumula los totales consolidando aportes de todas las instancias de Sum.

Como cada instancia de Sum envía un EOF al terminar, cada Aggregator espera recibir exactamente `SUM_AMOUNT` EOFs antes de calcular su top parcial. Esto se implementa con un contador `eof_count_by_client` por `client_id`: recién al llegar al valor esperado se calcula el top y se envía al Joiner.

Este diseño garantiza que cuando el Aggregator calcula su top parcial, ya recibió todos los datos de todas las instancias de Sum para ese cliente.

## Coordinación en el Joiner

El Joiner recibe un top parcial de cada Aggregator — uno por instancia — y los consolida en un top global. Como las frutas están particionadas sin superposición entre Aggregators, no hay sumas que realizar: simplemente se mergean las listas ordenadas y se toman los `TOP_SIZE` elementos de mayor cantidad. El resultado final se envía al Gateway una única vez por cliente, recién cuando llegaron los `AGGREGATION_AMOUNT` tops parciales.

## Escalabilidad respecto a los clientes

Cada mensaje interno lleva un `client_id` (UUID generado por el Gateway al conectarse cada cliente) que viaja a través de todo el pipeline. Cada componente — Sum, Aggregation y Joiner — mantiene estructuras de datos separadas por `client_id`, por lo que múltiples consultas de distintos clientes pueden estar en vuelo simultáneamente sin interferir entre sí. El Gateway maneja cada cliente en un proceso separado del pool y despacha el resultado al socket correcto al recibir la respuesta identificada por `client_id`.

## Escalabilidad respecto a la cantidad de controles

Al aumentar `SUM_AMOUNT`, RabbitMQ distribuye automáticamente más trabajo entre las instancias. El mecanismo de colas de control individuales por instancia escala linealmente — cada nueva instancia de Sum crea su propia `SUM_CONTROL_QUEUE_i` y cada instancia existente publica en ella al recibir un EOF.

Al aumentar `AGGREGATION_AMOUNT`, el hash routing redistribuye las frutas entre más particiones. El Joiner ajusta automáticamente cuántos tops parciales esperar antes de calcular el resultado final.