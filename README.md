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

## Тесты

```sh
go test ./...
```
