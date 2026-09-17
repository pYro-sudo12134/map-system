# Интеллектуальная система маршрутизации

Распределённая система для построения маршрутов и геопоиска.

## Основные компоненты

- RAG агент для распознавания в речи на естественном языке запросов, переводя их в JSON для поиска.
- Операторы для определения загрузки очередей. Чем больше сообщений в очереди, тем больше вызывается FaaS.
- FaaS для вычисления маршрутов и синхронизации баз данных.
- Маршрутизатор для направления запросов на обработку в AWS Transcribe, а затем на RAG агент, а также для получения сообщений и сохранения их в Redis.
- Сервис для авторизации и аутентификации.

## Поток обработки

```mermaid
sequenceDiagram
    autonumber
    actor Client as Клиент / Фронт
    participant Auth as Auth Service
    participant PG as PostgreSQL
    participant GW as Gateway
    participant Router as Map Router
    participant Redis as Redis (кеш маршрутов)
    participant S3 as S3
    participant TR as Transcribe
    participant Q as SQS Queues
    participant RAG as RAG Agent
    participant MapFaaS as Map FaaS
    participant SNS as SNS Topic

    Note over Client,PG: Фаза 1 — Авторизация
    Client->>Auth: Логин / пароль
    Auth->>PG: Проверка учётных данных
    PG-->>Auth: OK
    Auth-->>Client: Токен

    Note over Client,SNS: Фаза 2 — Обработка запроса
    Client->>GW: Запрос + токен + correlationId
    GW->>Router: Проксирование

    Note over Router,Redis: Поиск в кеше по correlationId
    Router->>Redis: GET маршрут по correlationId

    alt Cache HIT — маршрут найден
        Redis-->>Router: Готовый маршрут
        Router-->>Client: Ответ (из кеша)

    else Cache MISS — маршрута нет
        Redis-->>Router: null

        Note over Router,TR: Routing slip: S3 + Transcribe
        Router->>S3: Запись голосового файла
        Router->>TR: Запуск распознавания (routing slip)
        TR->>S3: Результат распознавания

        Router->>Q: Сообщение в RAG Queue (correlationId)
        Q->>RAG: Обработка
        RAG->>Q: Сообщение в Map FaaS Queue (correlationId)
        Q->>MapFaaS: Вычисление маршрута
        MapFaaS->>SNS: Публикация результата (correlationId)
        SNS->>Q: Router Result Queue (подписка)
        Q->>Router: Результат по correlationId

        Router->>Redis: SET маршрут по correlationId
        Router-->>Client: Ответ (вычисленный)
    end
```

## Масштабирование функций синхронизации

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Admin / Клиент
    participant Router as Map Router (AdminRoute)
    participant Auth as AdminAuthProcessor
    participant Throttle as Throttle
    participant SQS as SQS_SYNC Queue
    participant Collector as Sync Operator (Collector)
    participant Prom as Prometheus
    participant Rule as PrometheusRule
    participant AM as AlertManager
    participant FaaSNetes as faas-netes (OpenFaaS)
    participant Fn as faas-s (Sync FaaS)

    Note over Admin,Router: Admin вызывает sync-ручку
    Admin->>Router: POST /v1/admin/sync/{full|nodes|edges}
    Router->>Auth: adminAuthProcessor
    alt Не авторизован
        Auth-->>Router: UnauthorizedException
        Router-->>Admin: 401
    else Нет прав
        Auth-->>Router: ForbiddenException
        Router-->>Admin: 403
    else OK
        Auth-->>Router: OK
        Router->>Throttle: throttle(max, timePeriod)
        alt Лимит превышен
            Throttle-->>Router: 429
            Router-->>Admin: 429 Too Many Requests
        else OK
            Throttle-->>Router: OK
            Router->>Router: setBody {"sync_type":"full|nodes|edges"}
            Router->>SQS: sendMessage
            Router-->>Admin: 202 {"status":"..._sync_started"}
        end
    end

    Note over Collector,SQS: Sync Operator видит рост очереди
    loop Каждые CheckInterval
        Collector->>SQS: GetQueueAttributes (ApproximateNumberOfMessages)
        SQS-->>Collector: length
        Collector->>Prom: SetQueueLength("SQS_SYNC", length)
        Collector->>Prom: SetLastScrapeTimestamp("SQS_SYNC", now)
    end

    Note over Prom,Rule: PrometheusRule вычисляет алерты
    Prom->>Rule: evaluate sqs_sync_queue_length{queue_name="SQS_SYNC"}

    alt length > 50 (for 30s)
        Rule-->>AM: firing SQSSyncQueueVeryLong (critical)
    else length > 10 (for 30s)
        Rule-->>AM: firing SQSSyncQueueTooLong (warning)
    else length <= 10
        Rule-->>AM: resolved / no alert
    end

    Note over AM,FaaSNetes: AlertManager шлёт webhook на scale-function
    alt Алерт firing
        AM->>FaaSNetes: POST /system/scale-function/faas-s
        FaaSNetes->>Fn: scale up (до com.openfaas.scale.max=20)
    else Алерт resolved
        Note over FaaSNetes,Fn: send_resolved=false — webhook не шлётся,<br/>scale down делает OpenFaaS сам
    end

    Note over Fn,SQS: faas-s забирает сообщение и обрабатывает
    Fn->>SQS: ReceiveMessage
    Fn->>Fn: sync (full / nodes / edges)
    Fn->>SQS: DeleteMessage
    Note over Fn,SQS: Если не удалось — после N попыток → DLQ
```

## Масштабирование функций вычисления маршрутов

```mermaid
sequenceDiagram
    autonumber
    participant SQS as SQS_GO Queue
    participant Collector as Map Operator (Collector)
    participant Prom as Prometheus
    participant Rule as PrometheusRule
    participant AM as AlertManager
    participant FaaSNetes as faas-netes (OpenFaaS)
    participant Fn as faas-m (Map FaaS)

    Note over Collector,SQS: Оператор периодически скрейпит SQS_GO
    loop Каждые CheckInterval
        Collector->>SQS: GetQueueAttributes (ApproximateNumberOfMessages)
        alt Успех
            SQS-->>Collector: length
            Collector->>Prom: SetQueueLength(queue_name="SQS_GO", length)
            Collector->>Prom: SetLastScrapeTimestamp(queue_name="SQS_GO", now)
        else Ошибка
            Collector->>Prom: IncScrapeErrors(queue_name="SQS_MAP")
        end
    end

    Note over Prom,Rule: PrometheusRule вычисляет алерты
    Prom->>Rule: evaluate expr sqs_map_queue_length{queue_name="SQS_GO"}

    alt length > 50 (for 30s)
        Rule-->>AM: firing SQSMapQueueVeryLong (critical)
    else length > 10 (for 30s)
        Rule-->>AM: firing SQSMapQueueTooLong (warning)
    else length <= 10
        Rule-->>AM: resolved / no alert
    end

    Note over AM,FaaSNetes: AlertManager шлёт webhook на scale-function
    alt Алерт firing
        AM->>FaaSNetes: POST /system/scale-function/faas-m
        FaaSNetes->>Fn: scale up (до com.openfaas.scale.max)
    else Алерт resolved
        AM->>FaaSNetes: (send_resolved=false — webhook не шлётся)
        Note over FaaSNetes,Fn: Scale down делает OpenFaaS сам по своей логике
    end

    Note over Prom: ServiceMonitor скрейпит метрики оператора каждые 10s
    Prom->>Collector: scrape /metrics (interval 10s)
```



## Технологический стек

| Компонент | Технология | Назначение |
|-----------|-----------|-----------|
| **Network** | Traefik, Cilium, Envoy | Политики сетей и шлюзы |
| **Auth Service** | Java 21, Spring Boot | Авторизация и аутентификация пользователей |
| **Map Router** | Java 21, Camel, Spring Boot | Маршрутизация запросов внутри системы |
| **RAG Agent** | Python, Llama 3.2 | Преобразование текста в параметры |
| **Map FaaS** | Go, OpenFaaS | Маршруты, Neo4j, OpenSearch, SNS |
| **Sync FaaS** | Go, OpenFaaS | Синхронизация Neo4j и OpenSearch |
| **Map Operator** | Go | Метрики для автоскейлинга Map FaaS |
| **Sync Operator** | Go | Метрики для автоскейлинга Sync FaaS |
| **Neo4j** | Neo4j 5 | Графовая БД |
| **OpenSearch** | OpenSearch 2.11.0 | Поисковая копия, KNN, нечеткий поиск |
| **Redis** | Redis 8 | Кеш результатов, статусы запросов |
| **Liquibase** | Liquibase | Миграции |
| **PostgreSQL** | PostgreSQL 16 | Хранение пользователей |
| **LocalStack** | LocalStack 4 | SQS, SNS, S3, Transcribe, Cloudformation (локальная разработка) |
| **OpenFaaS** | OpenFaaS | Платформа для FaaS(функций как сервис) |
| **Prometheus** | Prometheus | Сбор метрик |
| **AlertManager** | AlertManager | Алерты для автомасштабирования по длине очередей |
| **cAdvisor** | cAdvisor | Мониторинг нагрузки | 
| **Grafana stack** | Tempo, Alloy, Grafana, Loki | Визуализация и сохранение мониторинга |
| **Helm** | Helm | CD |
| **ArgoCD** | ArgoCD | CD |
| **Kubernetes** | KinD/K3s | Оркестрация |
| **Terraform** | Terraform 1.15.5 | Возможность развертывания ресурсов вне кластера |
| **Longhorn** | Longhorn | Менеджер томов, бекапы |
| **Karmada** | Karmada | Мультикластер |
| **kpack, Harbor** | kpack, Harbor | Сборка и реестр образов |
