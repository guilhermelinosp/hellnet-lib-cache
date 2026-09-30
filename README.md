# hellnet-lib-cache

## 🧒 Entenda com 15 anos

*Seção introdutória em português para quem está começando — o restante deste
README segue em inglês.*

### A analogia

Cache é a **geladeira da casa**. O banco de dados é o **mercado**:

- Ir ao mercado toda hora gasta tempo e dinheiro — assim como consultar o banco
  a cada requisição.
- Guardar em casa o que você usa muito (leite, pão) torna tudo instantâneo.
- **L1** = a geladeira: fica na própria cozinha (memória do processo),
  rapidíssima, mas com espaço pequeno.
- **L2** = a despensa: fica no corredor de fora (Redis), muito maior, só um
  pouco mais longe.

### O problema que resolve

- Aplicações repetem as mesmas perguntas ao banco milhares de vezes ("tem
  leite?" — mil vezes por segundo).
- O cache responde **da memória**: em vez de ir ao mercado a cada pergunta,
  consulta primeiro a geladeira (L1) e depois a despensa (L2).
- E se o mercado fechar (banco de dados cair), os dados quentes continuam
  servíveis — a casa não para.

### Mini-dicionário

| Termo        | Analogia |
|--------------|----------|
| **hit**       | "Tinha na geladeira!" — achou no cache, resposta instantânea.                                               |
| **miss**      | "Acabou — fui ao mercado." Não estava em nenhuma camada; buscou direto na origem.                           |
| **TTL**       | A validade da embalagem: expirou, joga fora e busca um novo.                                                 |
| **eviction**  | Geladeira lotada: jogar fora o mais velho pra caber o novo.                                                  |
| **GetOrSet**  | Checa a geladeira; se estiver vazia, UMA pessoa vai ao mercado e divide com todos — os outros esperam a sacola em vez de ir juntos. |
| **stampede**  | Todo mundo correndo pro mercado porque acabou o leite — a lib impede isso por padrão.                        |
| **Healthy**   | "A geladeira e a despensa estão funcionando?" (`c.Healthy()` agrega a saúde das duas camadas).               |

### Primeiras linhas

```go
c, err := cache.New(ctx, tel) // carrega .env e HELLNET_CACHE_* sozinho; tel = instrumentação ou nil

var menu map[string]string
err = c.GetOrSet("menu-de-hoje", &menu, func(ctx context.Context) (any, error) {
	return pratoDoDia(), nil // só executa se der miss
}, time.Hour)
```

Linha por linha:

1. `cache.New(ctx, tel)` — monta a geladeira (L1) e a despensa (L2), cria um contexto
   interno e lê as variáveis de ambiente. Para propagar o contexto de uma
   requisição, use a variante `*Context`, como `GetContext`.
2. A biblioteca lê `HELLNET_CACHE_*`, com fallback para `HELLNET_*`.
   Toda operação roda com timeout interno
   (`Options.OperationTimeout`, padrão `5s`).
3. `GetOrSet("menu-de-hoje", ...)` — checa geladeira e despensa pela chave.
   Achou (**hit**)? Devolve pronto. Acabou (**miss**)? **UMA** pessoa cozinha (a
   factory) e divide com todos — os demais esperam a sacola em vez de correr
   juntos pro mercado (zero *stampede*).
4. O último argumento é o TTL — a validade individual desse prato no cardápio.

> Multi-layer cache library for Go — L1 (in-process memory), L2 (external,
> pluggable distributed backend).

Write-through, read-through, stampede protection, env-first configuration and
graceful degradation on backend failures.

## Install

```bash
go get github.com/guilhermelinosp/hellnet-lib-cache
```

Requires Go 1.27+.

## Quick start

### Env-first (recommended for microservices)

```go
package main

import (
	"context"
	"log"
	"time"

	cache "github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

func main() {
	// New(ctx, tel) loads .env and resolves HELLNET_CACHE_* before deciding L1/L2.
	c, err := cache.New(ctx, tel)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Set with per-key TTL
	if err := c.Set("order:1", Order{ID: "1", Name: "x"}, time.Hour); err != nil {
		log.Fatal(err)
	}

	// Get with the caller context propagated to tracing and L2 I/O.
	var o Order
	_ = c.GetContext(context.Background(), "order:1", &o)

	// GetOrSet — stampede-protected
	var cfg Config
	_ = c.GetOrSet("config:global", &cfg, func(context.Context) (any, error) {
		return loadConfig(), nil
	}, 0)

	// Remove / Exists
	_ = c.Remove("order:1")
	exists, _ := c.Exists("order:1")
	_ = exists
}
```

### Minimal env

```bash
export HELLNET_CACHE_CONNECTION=localhost:6379
# optional: export HELLNET_CACHE_PASSWORD=...
```

## Usage

```go
type OrderService struct{ cache cache.Cache }

func (s *OrderService) SetOrder(o Order) error {
	return s.cache.Set("order:"+o.ID, o, time.Hour)
}

func (s *OrderService) GetOrder(id string) (Order, error) {
	var o Order
	if err := s.cache.Get("order:"+id, &o); err != nil {
		return o, err
	}
	return o, nil
}

func (s *OrderService) Invalidate(id string) error {
	return s.cache.Remove("order:" + id)
}
```

### Context & timeouts

`New`/`MustNew` create a library-owned base context for compatibility methods
such as `Get`, `Set`, and background work (warming/touch). Context-aware
variants — `GetContext`, `SetContext`, `SetBytesContext`, `RemoveContext`,
`ExistsContext`, `GetOrSetContext`, and `HealthCheck` — propagate the caller's
context to tracing and L2 I/O. Each operation is bounded by
`OperationTimeout` (default `5s`, env-tunable via
`HELLNET_CACHE_OPERATION_TIMEOUT_MS`); L2 network calls additionally honor
`ConnectTimeout`/`ReadTimeout`. Calling `Close()` aborts library-owned work.

```go
c, err := cache.New(ctx, tel)
if err != nil {
    log.Fatal(err)
}
defer c.Close() // cancels the library-owned context
```

Additional runnable examples are available in the package documentation
(`go doc github.com/guilhermelinosp/hellnet-lib-cache/cache`).

> 🧒 L1 é a geladeira da cozinha; L2 é a despensa no corredor de fora.

## Idempotency

Run an operation at most once per key within a TTL and share the outcome with
every retry:

```go
result, executed, err := c.Idempotent("payment:order-42", 24*time.Hour,
	func() (any, error) {
		return chargeCard(order) // runs only when no completion record exists
	})
// executed=true  → fn ran now; its success was recorded for the TTL window
// executed=false → served from the recorded outcome; fn was not invoked
```

Failures are never recorded — a failed `fn` keeps the key free and the next
call retries it. Callers inside one process are stampede-protected via
singleflight; across instances deduplication is best-effort last-write-wins
(pair with `Lock` when strict cross-instance exclusivity matters).

> 🧒 Mesmo pedido duas vezes? Só uma execução acontece — as repetições recebem o
> resultado guardado até a validade (TTL) expirar.

## Rate limiting

Fixed-window counter enforced on the shared backend — every instance wired to
the same cache shares one budget:

```go
allowed, remaining, resetIn, err := c.Allow("api:user-7", 100, time.Minute)
if err != nil {
	return err // invalid limit/window configuration
}
if !allowed {
	// over budget: remaining==0; resetIn says when the window restarts
}
```

Without an external backend the same rules hold process-locally; transient
backend failures degrade to local counting with a warning instead of failing
hard.

> 🧒 Fila do parque: entram 100 por minuto, o painel mostra quantos ainda podem
> entrar e em quanto tempo a fila reinicia.

## Distributed locks

TTL-based mutual exclusion. `Lock` does not accept a caller context; its
backend calls use the library-owned context and operation timeout. Ownership is
token-checked: releases never delete somebody else's lease and a second release
errors (`ErrLockNotHeld`):

```go
unlock, ok, err := c.Lock("job:nightly-sync", 30*time.Second, 5*time.Second)
if err != nil {
	return err // backend failure — not contention
}
if ok {
	defer unlock()
}
// ok=false → another holder kept it for the whole wait window (nil unlock)
```

The lease auto-expires after ttl if the holder crashes (no renewal/watchdog in
v1: keep ttl above worst-case work). It is NOT fencing-safe for
strongly-consistent resources. Without an external backend, locking degrades to
process-local correctness only.

> 🧒 Placa de "ocupado" na porta: um de cada vez — e a placa cai sozinha se o
> dono desmaiar (TTL).


## Layers

| Layer | Provider           | Default TTL | Failure behavior                |
|-------|--------------------|-------------|---------------------------------|
| L1    | `MemoryProvider`   | 5 min       | Always healthy                  |
| L2    | `ExternalProvider` | 30 min      | Graceful — returns nil, logs    |

### Read-through

```
Get("key") → L1 → L2 → miss/nil
```

On a hit in L2, lower layers (L1) are populated automatically (async, deduplicated
warming).

### Write-through

```
Set("key") → L1.Set + L2.Set in parallel (WaitGroup)
```

> 🧒 Cada embalagem tem a própria validade.

## Per-key TTL

Each `Set`/`GetOrSet` accepts a `time.Duration` TTL. When `0`, the per-layer
fallback is used:

| Call              | L1        | L2        |
|-------------------|-----------|-----------|
| `Set(k, v)`       | 5min      | 30min     |
| `Set(k, v, 1h)`   | 1h        | 1h        |

L1 uses **absolute expiration** by default. Sliding is opt-in via
`L1SlidingExpiration`.

> 🧒 Acabou o leite? Uma só pessoa vai ao mercado — as outras esperam a sacola.

## Concurrency

| Mechanism           | Prevents                                            |
|---------------------|-----------------------------------------------------|
| Per-key semaphore   | Cache stampede in `GetOrSet` — 1 factory per key    |
| Auto-cleanup        | Semaphore disposed after factory (no leak)          |
| Warming dedup       | Only one warming task per key at a time             |
| Touch-on-read       | `TouchOnRead=true` extends TTL on all layers on hit |
| Parallel writes     | `WaitGroup` — Set/Remove across all layers          |

> 🧒 Mercado fechou? A cozinha segue funcionando com o que tem em casa.

## Resilience (L2)

| Mechanism            | Behavior                                                  |
|----------------------|-----------------------------------------------------------|
| Retry                | Exponential backoff with jitter (go-redis MaxRetries)    |
| Circuit breaker      | N consecutive failures → open → half-open (gobreaker)     |
| Degradation          | Every failure returns nil/false, never errors out         |

## Options

### Env vars (`HELLNET_CACHE_*`)

| Env var                              | Default              | Description                    |
|--------------------------------------|----------------------|--------------------------------|
| `HELLNET_CACHE_CONNECTION`           | *(optional)*         | External backend host:port     |
| `HELLNET_CACHE_PASSWORD`             | *(optional)*         | External backend password      |
| `HELLNET_CACHE_KEY_PREFIX`           | `hellnet:cache:`     | Key prefix in backend          |
| `HELLNET_CACHE_L1_DEFAULT_TTL`       | `00:05:00`           | L1 fallback TTL                |
| `HELLNET_CACHE_DEFAULT_TTL`          | `00:30:00`           | Global fallback TTL            |
| `HELLNET_CACHE_MAX_TTL`              | `24:00:00`           | Safety cap                     |
| `HELLNET_CACHE_TOUCH_ON_READ`        | `false`              | Auto-extend TTL on hit         |
| `HELLNET_CACHE_TOUCH_TTL`            | `00:10:00`           | Extension amount               |
| `HELLNET_CACHE_L1_SLIDING_EXPIRATION`| `false`              | Sliding vs absolute            |
| `HELLNET_CACHE_RETRY_COUNT`          | `2`                  | Max retry attempts             |
| `HELLNET_CACHE_RETRY_BASE_DELAY_MS`  | `200ms`              | Bare integer means milliseconds|
| `HELLNET_CACHE_CB_FAILURES`          | `5`                  | Circuit breaker threshold      |
| `HELLNET_CACHE_CB_DURATION_SEC`      | `30s`                | Bare integer means seconds     |
| `HELLNET_CACHE_OPERATION_TIMEOUT_MS` | `5000`               | Bare integer means milliseconds|
| `HELLNET_CACHE_ENABLE_L1`            | `true`               | Enable L1                      |
| `HELLNET_CACHE_ENABLE_L2`            | `true`               | Enable L2                      |

Variables accept Go duration syntax (`5m`, `30s`) or clock-style (`00:05:00`).
Variables ending in `_MS` or `_SEC` also accept bare integers in their declared unit.
A defined invalid duration returns an error from `New`; it no longer silently uses the default.

When `HELLNET_CACHE_ENABLE_L2=true` is explicit, missing `HELLNET_CACHE_CONNECTION` is an error.
When L2 is not explicitly enabled, the cache keeps its memory-only fallback and emits one warning.

For dependency injection, use `NewWithOptions` with `WithOptions`, `WithProviders`, and `WithSerializer`.
`New` keeps loading `.env` for backward compatibility; `NewWithOptions` does not load dotenv files.

## Dependencies

- `github.com/dgraph-io/ristretto/v2` — L1 memory provider
- `github.com/redis/go-redis/v9` — L2 external backend
- `github.com/sony/gobreaker` — Circuit breaker (L2 resilience)

Test-only dependencies: `github.com/alicebob/miniredis/v2` provides hermetic
Redis behavior tests, and `go.uber.org/goleak` checks for leaked goroutines.

## Observabilidade

Passe `*telemetry.Telemetry` via `WithInstrumentation`:

```go
cache, err := cache.NewWithOptions(ctx,
    cache.WithOptions(options),
    cache.WithInstrumentation(tel),
)
```

Spans ctx-first: `cache.get`, `cache.set`, `cache.remove`, `cache.exists`,
`cache.get_or_set` e `cache.health`. Métricas: `hellnet.cache.operations` e
`hellnet.cache.operation.duration` (`s`), com `operation` e `result`.
`HealthCheck(ctx)` é para `/health`; cache degradada não deve, por si só,
determinar `/ready`.

## License

Apache 2.0 © 2026 Hellnet
