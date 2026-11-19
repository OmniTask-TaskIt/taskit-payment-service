# Task It — Payment Service

Microservicio de pagos de Task It. Orquesta el cobro, la retencion y la liberacion del dinero de una tarea contra Mercado Pago, sin custodiar fondos y sin ver datos de tarjeta.

El repositorio incluye dominio, casos de uso, adaptadores, API y el test de idempotencia del webhook.

## Proposito

Payment Service es el unico componente que habla con Mercado Pago. El resto de la plataforma se entera del dinero por eventos.

El modelo es de cero custodia:

- Task It no guarda saldos ni funciona como billetera.
- Task It no guarda PAN, CVV, vencimiento ni tokens de tarjeta. La tokenizacion ocurre en Mercado Pago.
- El cobro del demandante queda retenido en la pasarela (escrow) hasta que la tarea se completa o un administrador HITL resuelve una disputa.
- El reparto usa Split Payments: el prestador es el collector y la plataforma declara su comision (`marketplace-fee`).

## Arquitectura

Arquitectura hexagonal. El dominio no importa Ring, JDBC, Carmine ni el SDK de la pasarela.

```text
src/taskit/payment/
  domain/          esquemas Malli, errores y logica pura
  ports/           defprotocol de entrada y de salida
  application/     casos de uso
  adapters/        Postgres, Redis, Mercado Pago, RabbitMQ
  api/             rutas Reitit e interceptor de errores
test/taskit/payment/
```

| Capa | Responsabilidad |
| --- | --- |
| Dominio | Estados, transiciones y contratos. Funciones puras que devuelven un mapa de decision. |
| Puertos | Lo que la aplicacion ofrece (`ports.inbound`) y lo que necesita del mundo (`ports.outbound`). |
| Aplicacion | Orquesta puertos dentro de una transaccion ACID. No conoce HTTP ni SQL. |
| Adaptadores | Implementan los puertos: `next.jdbc`, HoneySQL, Carmine, Langohr, cliente HTTP. |
| API | Reitit + Muuntaja. Un interceptor traduce las excepciones de dominio a JSON. |

Integrant arranca el sistema. Al iniciar, `load-dotenv!` lee `.env` y lo publica como propiedades de la JVM. Aero toma cada valor del entorno del proceso y, si no esta, de esa propiedad. Una variable ya exportada en el sistema gana sobre el archivo.

## Dominio

Estados de la tarea, tal como los emite el MS de Tareas:

`ACTIVE`, `ASSIGNED`, `SEMI_COMPLETED`, `COMPLETED`, `CANCELLED`, `REPORTED`, `DISPUTED`.

Estados de la transaccion:

| Estado | Significado |
| --- | --- |
| `PENDING` | Preferencia creada. Todavia no hay fondos retenidos. |
| `RETAINED` | Mercado Pago informo `approved`. El dinero queda en escrow. |
| `RELEASED` | El prestador recibio la liquidacion. |
| `REFUNDED` | El demandante recibio la devolucion. |
| `REJECTED` | El cobro fue rechazado. Extension del modelo de referencia, necesaria para `PaymentFailedEvent`. |

`currency` vive en la tarea y en la transaccion. Sin moneda, un precio no se puede cobrar. Monedas previstas: `COP`, `ARS`, `BRL`, `MXN`, `CLP`, `PEN`, `UYU`. El monto es un entero en la unidad que ya usa el precio de la tarea (en COP, pesos).

Reglas que ya estan en `taskit.payment.domain.logic`:

- El checkout solo se arma con la tarea en `SEMI_COMPLETED`, con prestador asignado, pagador igual al dueno, cobrador igual al asignado, y monto y moneda iguales a la tarea.
- Un checkout `PENDING` con los mismos terminos se reexpone. Si la preferencia aun no existe, el caso de uso puede reanudarlo. `RETAINED`, `RELEASED` y `REFUNDED` no abren otro cobro. `REJECTED` si permite un intento nuevo.
- Un webhook duplicado se descarta. La clave es el SHA-256 de `payment_id` (`idempotency:mp:webhook:<hex>`) y el TTL es de 24 horas. Lo aplica Redis; la decision pura esta en `webhook-delivery-decision`.
- `approved` retiene. `rejected` y `cancelled` rechazan. `pending`, `in_process` y `authorized` no mueven estado. `charged_back` y un status desconocido quedan en conflicto, sin transicion automatica.
- Liberar exige tarea `COMPLETED` y transaccion `RETAINED`. Repetir la liberacion no vuelve a emitir el evento.
- Una disputa solo la ejecuta `actor-role = ADMIN` y solo sobre fondos `RETAINED`. `REFUND_PAYER` reembolsa. `RELEASE_PAYEE` liquida y emite `FundsReleasedEvent`.
- El historial proyecta rol `PAYER` o `PAYEE`, monto, moneda y estado. No incluye preferencia ni identificadores utiles para reconstruir una tarjeta.

Los comandos propios usan mapas Malli cerrados. Una clave de tarjeta (`pan`, `cvv`, `card_number`, etc.) rechaza el comando con `CARD_DATA_FORBIDDEN`.

## Eventos RabbitMQ

Exchange de dominio: `taskit.events`. Dead letter: `taskit.dlx`. Cola de este servicio: `payment.events`. Routing key de publicacion: `taskit.payment.events`.

Los nombres se leen del entorno (ver `.env.example`).

### Consume

| Evento | Efecto |
| --- | --- |
| `TaskStatusChangedEvent` | `SEMI_COMPLETED` habilita el checkout. `COMPLETED` dispara la liberacion del escrow. |
| `DisputeResolvedEvent` | `REFUND_PAYER` o `RELEASE_PAYEE`, emitido por Auditoria / HITL. |

### Publica

| Evento | Cuando |
| --- | --- |
| `PaymentAuthorizedEvent` | Mercado Pago confirmo el cobro y los fondos quedaron retenidos. |
| `FundsReleasedEvent` | El dinero se liquido al prestador (cierre normal o disputa a su favor). |
| `PaymentFailedEvent` | El cobro fue rechazado. |

Un reembolso actualiza la transaccion a `REFUNDED` y no publica un cuarto tipo de evento. El historial es la fuente para ese estado.

### Contratos para los otros microservicios

**MS Tareas** escucha `PaymentAuthorizedEvent` para marcar la tarea en ejecucion asegurada, y `PaymentFailedEvent` para avisar al demandante.

**MS Perfiles** y **MS Auditoria (HITL)** escuchan `FundsReleasedEvent` para la contabilidad del prestador, y `PaymentFailedEvent` para la trazabilidad de cobros rechazados.

Ejemplo de `PaymentAuthorizedEvent`:

```json
{
  "event-id": "6f1c2a30-9b4e-4d1a-8c77-0a1b2c3d4e5f",
  "event-type": "PaymentAuthorizedEvent",
  "occurred-at": "2026-11-16T18:00:00Z",
  "transaction-id": "8a1b0c2d-3e4f-4a5b-9c6d-7e8f90123456",
  "task-id": "11111111-1111-4111-8111-111111111111",
  "payer-id": "22222222-2222-4222-8222-222222222222",
  "payee-id": "33333333-3333-4333-8333-333333333333",
  "amount": 150000,
  "currency": "COP",
  "mp-payment-id": "1234567890",
  "mp-preference-id": "pref-abc"
}
```

`TaskStatusChangedEvent` viaja con la foto de la tarea (`task-id`, nombre, estado anterior, estado nuevo, dueno, asignado, categoria, precio y moneda). `DisputeResolvedEvent` trae `outcome`, `actor-id`, `actor-role` y `reason`. Los esquemas cerrados estan en `src/taskit/payment/domain/model.clj`.

## Politicas PN-PAY

- **Cero tenencia y cero PCI.** No hay tabla de saldos ni de tarjetas. El adaptador de Mercado Pago pide y guarda solo ids de preferencia y de pago.
- **ACID.** Cada cambio de estado financiero se confirma en PostgreSQL dentro de `TransactionManagerPort/in-transaction`. El evento sale en la misma transaccion a la tabla `outbox` y se publica despues del commit.
- **Idempotencia.** El webhook responde en el borde. Si la clave ya esta en Redis se descarta (`{:status :duplicate}`). El objetivo de tiempo es menos de 2 segundos. El cliente HTTP usa circuit breaker, timeout y un reintento con backoff (`resilience4clj`). Un pago `pending` libera la clave para que la notificacion `approved` siguiente pueda procesarse.
- **Errores.** Los rechazos de negocio son mapas `{:status :rejected :code :message :details}`. El borde HTTP los convierte en JSON. Codigos: `INVALID_PAYLOAD`, `CHECKOUT_NOT_ALLOWED`, `CHECKOUT_CONFLICT`, `ALREADY_PAID`, `MISSING_PAYEE`, `CARD_DATA_FORBIDDEN`, `INVALID_TRANSITION`, `TRANSACTION_NOT_FOUND`, `AMOUNT_MISMATCH`, `CURRENCY_MISMATCH`, `UNAUTHORIZED_HITL`, `GATEWAY_CONFLICT`, `GATEWAY_UNAVAILABLE`, `TASK_MISMATCH`.

## API

El JSON propio del servicio usa claves en kebab-case. Mercado Pago sigue mandando su cuerpo a `POST /webhooks/mercadopago`; tambien se acepta el aviso por query `topic` e `id`.

| Metodo y ruta | Uso |
| --- | --- |
| `GET /health` | Proceso vivo. |
| `POST /payments/checkout` | Crea o reexpone la preferencia Split. La tarea tiene que haber llegado antes como `SEMI_COMPLETED`. |
| `POST /payments/escrow/release` | Libera el escrow si la tarea esta `COMPLETED`. |
| `POST /payments/disputes` | Ejecuta `DisputeResolvedEvent`. |
| `GET /payments/history?user-id=` | Historial del usuario, sin datos de tarjeta. |
| `POST /webhooks/mercadopago` | Notificacion de pago. Un duplicado responde `200` con `status: duplicate`. |

Con `RABBITMQ_ENABLED=true` el servicio consume `taskit.task.events` y `taskit.audit.events`, y publica en `taskit.payment.events`.

Mercado Pago se llama con circuit breaker, un reintento y un limite de 1.6 segundos. La preferencia envia `marketplace_fee` y `external_reference`. Liberar hace `PUT /v1/payments/{id}` con `capture: true`. Reembolsar hace `POST /v1/payments/{id}/refunds`. Si el cobro ya estaba capturado o reembolsado, la operacion se toma como hecha.

## Ejecutar en local

Requisitos: Docker, Java 17 o superior y [Clojure CLI](https://clojure.org/guides/install_clojure).

```powershell
docker compose up -d
clojure -M:test
clojure -M -m taskit.payment.core
```

`.env` ya esta en la raiz y no se versiona. Si falta, copialo desde el ejemplo: `Copy-Item .env.example .env`. Con `RABBITMQ_ENABLED=false` el entorno local es Postgres, Redis y la outbox. La contrasena `password` de Postgres es la del `docker-compose.yml` de desarrollo. Aero lee `resources/config.edn`. El puerto por defecto es `8080`.

`clojure -M:test` corre el webhook con `with-redefs` sobre la pasarela y la base. La llave de idempotencia se comprueba con el mismo contrato que Redis: alta con TTL de 24 horas, descarte de la copia y liberacion si el pago sigue pendiente o si la red falla.

## Secretos

Las credenciales no van en el codigo ni en git. `.env` esta en `.gitignore`. `.env.example` solo tiene placeholders.

Si un token de Mercado Pago o una URL de CloudAMQP con usuario y contrasena circularon en un ticket o en un chat, hay que revocarlos en el proveedor y cargar los nuevos solo en el entorno de ejecucion.
