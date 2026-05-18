---
marp: true
theme: default
paginate: true
---
# Реализация драйвера YDB через C-биндинг

---

# Постановка задачи

- Воспроизводимое dev-окружение (Docker/Compose + devcontainer)
- PnP архитектура, один .so без внешних зависимостей для обновления версии
- Вызов функций через механизмы FFI (POD-типы, C-language linkage)

---

# Ядро драйвера

- Реализован жизненный цикл драйвера: конфиг, запуск, ожидание готовности
- Добавлен Query API: выполнение запросов с/без транзакций, интерфейс для работы с повторением операций
- Введён универсальный сборщик параметров (scalar/list/struct) для параметризованных запросов
- Добавлено чтение result set для базовых типов (int/uint/double/bool/utf8/bytes)
  - потоково
  - с копированием данных

---

# Текущее состояние проекта

- Рабочий C драйвер поверх C++ SDK
- Поддержаны ключевые API: driver, query execution, params, retry
- Интеграционные и unit тесты

---

# Дальнейшие планы

- Расширение покрытия API
- Некоторые параметрические типы пока не поддерживаются (decimal)
- Параметризованная настройка клиента
- Небольшие оптимизации в CI

---

# Программа на C

```c
  int32_t key = 52;
  const char *value = "six seven";
  st = ydb_params_set_uint64(params, "$key", key, rd);
  check_status(st, "params_set_uint64 $key", rd);
  st = ydb_params_set_utf8(params, "$value", value, rd);
  check_status(st, "params_set_utf8 $value", rd);
  st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, &tx, rd);
  check_status(st, "begin_tx (UPSERT)", rd);
  st = ydb_query_tx_execute(
      tx, "DECLARE $key AS Uint64;\n"
      "DECLARE $value as Utf8;\n"
      "UPSERT INTO users (key, value) VALUES ($key, $value)", params, NULL, rd);
  check_status(st, "execute UPSERT", rd);
  st = ydb_query_tx_commit(tx, rd);
  check_status(st, "commit UPSERT", rd);
  ydb_query_tx_free(tx, rd);
```

---

# Пример аналогичной программы на python

```python
    st = ydb_params_set_uint64(params, b("$key"), key, rd)
    check_status(st, "params_set_uint64 $key", rd)
    st = ydb_params_set_utf8(params, b("$value"), b(value), rd)
    check_status(st, "params_set_utf8 $value", rd)
    st = ydb_query_begin_tx(qc, YDB_TX_SERIALIZABLE_RW, ctypes.byref(tx), rd)
    check_status(st, "begin_tx (UPSERT)", rd)
    st = ydb_query_tx_execute(tx,b(
            "DECLARE $key AS Uint64;\n"
            "DECLARE $value as Utf8;\n"
            "UPSERT INTO users (key, value) VALUES ($key, $value)"),
        params,None,rd)
    check_status(st, "execute UPSERT", rd)
    st = ydb_query_tx_commit(tx, rd)
    ydb_query_tx_free(tx, rd)
```

---


<style>
.columns {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1rem;
}
</style>

# Вопросы

<div class="columns">
<div>

### Команда
- Бабаков Илья, Б13-404

### Ментор
- Тимофей Кулин, Yandex

</div>
<div>

![w:350](./images/qr-code.png)

</div>
</div>