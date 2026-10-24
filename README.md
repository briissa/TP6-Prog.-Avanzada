# TP6 - Activación de servicios con Kafka

POC local de una saga de activación basada en eventos. El único punto de entrada es
`activation-api`; facturación y aprovisionamiento consumen en paralelo
`ActivationRequested` y publican sus resultados en Kafka.

## Ejecutar

Requiere Docker Desktop y Docker Compose:

```bash
docker compose up --build
```

- Demo UI: http://localhost:5173
- API: http://localhost:3000
- Kafka UI: http://localhost:8080
- MailHog: http://localhost:8025

La UI permite elegir un fallo de facturación o aprovisionamiento. En el camino feliz,
la activación termina en `ACTIVE`; ante un fallo, termina en `FAILED` y facturación
compensa la cuenta. La timeline se actualiza por WebSocket.

## API

- `POST /activations` — acepta `customerId`, `planId` y `simulateFailure`
  (`none`, `billing` o `provisioning`); responde `202` con `activationId` y `PENDING`.
- `GET /activations/:id` — devuelve estado, pasos e historial de eventos.

## Servicios y confiabilidad

Compose levanta Kafka en KRaft, Kafka UI, PostgreSQL, MailHog, la API, Billing,
Provisioning, Notifications, CRM Analytics y la UI. Los servicios usan bases
PostgreSQL separadas. Los contratos de eventos están en `libs/contracts`.

Los consumidores guardan cada `eventId` procesado junto con sus efectos para evitar
duplicados. Los productores persisten en un outbox transaccional; los mensajes con
errores técnicos se reintentan con backoff y, tras agotar los intentos, se envían a
la DLQ del topic. CRM Analytics usa un consumer group dedicado que comienza desde el
inicio del log para registrar los eventos retenidos.

## Escenarios de demo

1. **Camino feliz:** contratar desde la UI sin simular fallo; verificar los cuatro
   eventos de la saga y el correo en MailHog.
2. **Fallo y compensación:** seleccionar Facturación o Aprovisionamiento; comprobar
   `FAILED`, el evento terminal y la cuenta cancelada en Billing.
3. **Servicio detenido:** ejecutar `docker compose stop notification-service`,
   crear tres activaciones y volver a iniciarlo con
   `docker compose start notification-service`; los correos pendientes se procesan
   al reconectarse el mismo consumer group.
4. **Escalado:** ejecutar `docker compose up --scale billing-service=2 -d` y observar
   en Kafka UI la asignación de particiones al consumer group `billing-svc`.
5. **Replay:** ejecutar un consumer con un group ID nuevo para leer desde el
   comienzo el historial de eventos de activación:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:19092 \
     --topic activation.events \
     --group loyalty-demo \
     --from-beginning
   ```
