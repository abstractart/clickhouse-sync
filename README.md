# clickhouse-sync

Утилита для переноса партиции таблицы на указанный диск на **всех узлах**
ClickHouse-кластера (шардах и репликах внутри шардов).

Как это работает:

1. Подключается к узлу входа (`-hostname`) по HTTPS и запрашивает
   `system.clusters`, чтобы получить имена всех узлов кластера.
2. Поочерёдно подключается к каждому узлу и выполняет
   `ALTER TABLE <db>.<table> MOVE PARTITION '<partition>' TO DISK '<disk>'`.

Реализация — только стандартная библиотека Go. Отдельный ClickHouse-клиент не
используется, общение идёт через HTTP-интерфейс ClickHouse поверх TLS.

## Сборка

```sh
go build -o clickhouse-sync .
```

## Использование

```sh
./clickhouse-sync \
  -user default \
  -password secret \
  -hostname ch-node-1.example.com \
  -database analytics \
  -table events \
  -partition 2024-01 \
  -destination-disk cold
```

Пароль можно передать через переменную окружения `CLICKHOUSE_PASSWORD` вместо
флага `-password`.

### Флаги

| Флаг                  | Описание                                                            | По умолчанию |
| --------------------- | ------------------------------------------------------------------- | ------------ |
| `-user`               | Имя пользователя ClickHouse (обязательный)                          | —            |
| `-password`           | Пароль (или `$CLICKHOUSE_PASSWORD`)                                 | —            |
| `-hostname`           | Узел входа для определения топологии кластера (обязательный)        | —            |
| `-port`               | HTTPS-порт ClickHouse                                               | `8443`       |
| `-database`           | Имя базы данных (обязательный)                                      | —            |
| `-table`              | Имя таблицы (обязательный)                                          | —            |
| `-partition`          | Значение партиции (обязательный)                                    | —            |
| `-destination-disk`   | Имя целевого диска (обязательный)                                   | —            |
| `-cluster`            | Имя кластера в `system.clusters`; пусто — все известные узлы        | —            |
| `-partition-id`       | Трактовать `-partition` как id партиции (`MOVE PARTITION ID`)       | `false`      |
| `-insecure`           | Не проверять TLS-сертификат                                         | `false`      |
| `-dry-run`            | Показать запросы, но не выполнять перенос                           | `false`      |
| `-continue-on-error`  | Не останавливаться при ошибке на узле                               | `false`      |
| `-timeout`            | Таймаут одного запроса                                              | `5m`         |

### Пример dry-run

```sh
./clickhouse-sync -user default -hostname ch-node-1 \
  -database analytics -table events -partition 2024-01 \
  -destination-disk cold -dry-run
```

## Примечания

- `MOVE PARTITION TO DISK` — локальная операция на данных конкретного узла,
  поэтому она выполняется на каждом узле кластера отдельно.
- Если в `system.clusters` несколько кластеров, укажите нужный через `-cluster`.
- По умолчанию при первой же ошибке выполнение прерывается; используйте
  `-continue-on-error`, чтобы пройти по всем узлам несмотря на сбои.
- **Идемпотентность.** Повторный запуск безопасен: если партиция уже целиком
  лежит на целевом диске, ClickHouse отвечает ошибкой 479 (`All parts of
  partition ... are already on disk ...`) — mover трактует её не как сбой, а
  как `SKIP`. Такой узел не считается упавшим, код возврата остаётся `0`, а в
  итоговой строке видно, сколько партиций реально перенесено и сколько уже были
  на месте: `Done: ... (N moved, M already there)`. Прочие ошибки (нет таблицы,
  узел недоступен и т.п.) по-прежнему считаются сбоями.

## Локальная инфраструктура (docker compose)

В каталоге `deploy/` описан полноценный стенд для проверки переноса:

- **ClickHouse-кластер `cluster_2s_2r`** — 2 шарда × 2 реплики (4 узла:
  `clickhouse-s1r1/s1r2/s2r1/s2r2`).
- **ClickHouse Keeper** — координация репликации и `ON CLUSTER` DDL.
- **SeaweedFS** — S3-хранилище для диска `object_storage`.
- Два диска в каждом узле: `local` (локальный ФС) и `object_storage` (S3),
  объединённые в политику хранения `tiered`.
- **Шардируемая и реплицируемая таблица** `demo.events_local`
  (`ReplicatedMergeTree`) + распределённая `demo.events` (`Distributed`).
- HTTPS включён на каждом узле (самоподписанный сертификат), поэтому mover
  ходит строго по HTTPS (с флагом `-insecure`).

Данные при вставке попадают на диск `object_storage`, а mover переносит
партицию на диск `local`.

### Быстрый старт

```sh
make demo
```

`make demo` последовательно выполняет: `up` → `init` → `show` → `move` → `show`,
так что видно размещение партиций на дисках **до** и **после** переноса.

### По шагам

```sh
make up      # поднять кластер + SeaweedFS (ждёт healthcheck'ов)
make init    # создать таблицы и загрузить демо-данные на object_storage
make show    # показать размещение партиций по узлам и дискам
make move    # перенести партицию 202401 с object_storage на local на всех узлах
make show     # убедиться, что партиция 202401 теперь на диске local
make bucket   # показать содержимое S3-бакета (пустеет по мере переноса)
make truncate # полностью очистить demo.events_local (чистый лист для ручных тестов)
make client   # открыть интерактивный clickhouse-client на узле шарда №1
make clean    # остановить и удалить тома
```

> `make truncate` очищает таблицу на всех узлах (`TRUNCATE ... ON CLUSTER`).
> Чтобы снова залить демо-данные на `object_storage`, повторите `make init`.

### Просмотр S3-бакета

Сервис `bucket` перечисляет объекты в бакете SeaweedFS, на котором держится диск
`object_storage`. По мере переноса партиций на `local` ClickHouse удаляет их
части из S3, и бакет пустеет:

```sh
make bucket
# то же напрямую:
docker compose -f deploy/docker-compose.yml --profile tools run --rm bucket
```

```text
Bucket s3://clickhouse : 48 object(s), 21712 byte(s) total   # до переноса
Bucket s3://clickhouse : 4 object(s), 4 byte(s) total        # после (остаются лишь служебные маркеры)
```

### Подключение через clickhouse-client

Сервис `client` открывает интерактивный `clickhouse-client`, уже подключённый к
узлу шарда №1 (`clickhouse-s1r1`) с зашитыми кредами (`default`/`secret`, база
`demo`) — указывать ничего не нужно:

```sh
make client
# то же самое напрямую:
docker compose -f deploy/docker-compose.yml --profile tools run --rm client
```

Можно прокинуть и разовый запрос — аргументы добавляются к готовой команде:

```sh
docker compose -f deploy/docker-compose.yml --profile tools run --rm client \
  --query "SELECT hostName(), partition, disk_name FROM system.parts WHERE active"
```

`make move` под капотом запускает mover внутри docker-сети кластера:

```sh
docker compose -f deploy/docker-compose.yml --profile tools run --rm sync \
  -user default -password secret \
  -hostname clickhouse-s1r1 \
  -database demo -table events_local \
  -partition 202401 -partition-id \
  -destination-disk local -insecure
```

> Перенос выполняется над локальной таблицей `events_local` (не над
> `Distributed`), поскольку `MOVE PARTITION` — локальная операция и не
> реплицируется; поэтому mover проходит по всем 4 узлам.

## Тесты

Быстрые модульные тесты (Docker не нужен):

```sh
go test ./...
# или
make test
```

Интеграционные тесты поднимают настоящий ClickHouse через
[testcontainers-go](https://golang.testcontainers.org/) и проверяют весь
сценарий на реальном сервере: обнаружение узлов через `system.clusters`, перенос
партиции между дисками (`MOVE PARTITION ... TO DISK`) и идемпотентность
(повторный запуск возвращает `ErrAlreadyOnTarget`). Требуется запущенный Docker:

```sh
go test -tags=integration ./...
# или
make test-integration
```

Они автоматически заменяют ручную проверку через команды `make` — узел
конфигурируется с HTTPS и storage-политикой из двух дисков (`default` + `cold`),
данные загружаются, партиция переносится, результат проверяется.
