# Архитектура HTTP-прокси

```mermaid
flowchart TB
    C[Клиенты] --> A[Server: accept + poll]
    S((StopSignal pipe)) -. SIGINT/SIGTERM .-> A
    A -->|trySubmit| Q[(Очередь backlog)]
    A -->|очередь полна| R503[503 + close]
    Q --> CP[Client ThreadPool]
    CP --> CS[ClientSession]
    CS --> RR[RequestReader + picohttpparser]
    CS -->|GET| CA[Cache.acquire]
    CA -->|hit / ожидание| ER[EntryReader]
    CA -->|miss| FP[Fetch ThreadPool]
    FP --> RF[runFetch]
    RF --> U[UpstreamClient]
    U --> O[(Целевой сервер)]
    RF -->|head и данные| CE[CacheEntry]
    CE -->|по мере поступления| ER
    ER --> CS
    CS -->|PUT/POST/DELETE| U
    M[Cache maintenance thread] -->|рост ёмкости, TTL, LRU| CA
    L[Logger] -.-> CS
    L -.-> RF
    L -.-> M
```

## Модули

| Файлы      | Классы                                                           |
| ---------- | ---------------------------------------------------------------- |
| `Base.hpp` | ошибки, `UniqueFd`, `Platform` (Linux/macOS)                     |
| `Logger`   | `Logger`                                                         |
| `Runtime`  | `StopSignal`, `SignalHandler`, `ThreadPool`                      |
| `Net`      | `Socket` (poll + таймауты + остановка)                           |
| `Http`     | `HttpRequest`, `ResponseHead`, `RequestReader`, `UpstreamClient` |
| `Cache`    | `CacheEntry`, `EntryReader`, `Cache` (бюджет памяти, TTL, LRU)   |
| `Proxy`    | `ClientSession`, `Server`                                        |
| `Config`   | `Config`, `ConfigParser`                                         |

## Потоки

- **main**: `poll(listen, stop-fd)`, принимает соединения и кладёт сессии в ограниченную очередь. При переполнении отвечает 503.
- **Client pool** (`--max-client-threads`): читает запрос, отдаёт данные клиенту. Сессий может быть больше потоков за счёт очереди.
- **Fetch pool**: загрузка с целевого сервера для GET. Одна запись кэша даёт один запрос на сервер.
- **Cache maintenance**: спит на condition variable (таймер TTL/2, не реже раза в секунду, или запрос на память) и выполняет рост ёмкости, чистку просроченных записей и LRU-вытеснение.

## Кэш

- Ключ: `http://host[:port]/path?query`. Query string входит в ключ, потому что разные query обычно дают разные ресурсы.
- Кэшируются только ответы 200 на GET. Остальные коды и методы передаются насквозь, запись из кэша убирается.
- Ресурс больше `--cache-max-size` передаётся насквозь через окно 256 КБ с обратным давлением.
- MIME-метка хранится в сериализованных заголовках ответа.
- Ёмкость растёт вдвое до `--cache-max-size`, затем вытесняется LRU. Записи с читателями или в загрузке не вытесняются.
- TTL отсчитывается с момента полной загрузки.
- Несколько клиентов на один URL ждут одну загрузку. Если один отвалился, остальные продолжают. Если отвалились все, загрузка отменяется.
