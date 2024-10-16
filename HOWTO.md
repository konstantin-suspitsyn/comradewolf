## Передача описания таблиц модулю

Для одного куба таблицы должны храниться в одной папке. Структура папок:
```
CUBE_NAME/ # Папка, в которой будут содержаться описания таблиц кубв
├─ data/ # Содержит *.toml файлы-описания фактовых таблиц
│ ├─ file.toml # Имя не имеет значение.
│ └─ ...
├─ dimension/ # Содержит описание *.toml таблиц-словарей
│ ├─ file.toml # Имя не имеет значение.
└─└─ ...
```
### Фактовые таблицы (папка data)
Структура файла:
```
table = "название таблицы"
schema = "название схемы"
database = "название базы данных"
base_table = "является ли базовой таблицей без группировок"

[fields]
название_поля_х = {field_type = "service_key", alias = "sk_id_game", calculation_type = "none", following_calculation = "none", front_name = "none", data_type="number"}
```
- **название_поля_х** — название поля в БД
- **field_type** — тип поля. может быть одним из значений OlapFieldTypes(): 
  - **service_key** используется для соединения фактовых таблиц из папки data и словарей из папки dimension. Другие соединения запрещены
  - **value** используется для обозначения числовых показателей
  - **dimension** для текстовых полей, дат, дат-времен и прочих нечисловых значений
- **alias** — альтернативное название поля. Нужно для синхронизации названий полей между таблицами. Должно быть одинаковое для единых сущностей. Изначально, должно быть проставлено для base_table, тиражировано по всем фактовым таблицам и словарям. <br>_Пример:_<br>В фактовой таблице есть кол-во продаж в штуках. Мы присваиваем alias="sales_pcs". Во всех агрегатах, где есть продажи в штуках, мы присваиваем alias="sales_pcs" вне зависимости агрегированное это поле или нет. Главное — единая сущность
- **calculation_type** — типы агрегации, которые произведены над полем. Должен быть один из списка OlapCalculations()
- **following_calculation** — возможность использования дальнейшей калькуляции над существующей калькуляции.<br>_Пример:_<br>Есть таблица, которая была сформирована запросом select **dim_1, dim_2, sum(sales) as sales_sum from t1**, для этой таблицы, скорее всего можно сделать **select t2.dim_1, sum(t2.sales_sum) from (dim_1, dim_2, sum(sales) as sales_sum from t1) as t2**. А дальнейшей калькуляции над полем **count_distinct_dim3** нельзя из запроса **select dim_1, dim_2, count(distinct dim3) as count_distinct_dim3 from t1**
- **front_name** — название для фронтенда. Если на front выводить не надо, необходимо проставить "none"
- **data_type** — один из OlapDataType()

### Таблицы-словари/справочники (папка dimension)
```
table = "название таблицы"
schema = "название схемы"
database = "название базы данных"

[fields]
название_поля_х = {field_type = "service_key", alias = "sk_id_developer", front_name="none", data_type="number"}
```

## Данные из Frontend

Пустой JSON из frontend должен иметь структуру
```
{'SELECT': [], 'CALCULATION': [], 'WHERE': []}
```

### Where

Числовые значения могут быть как текстовыми, так и числовыми. Ставить кавычки или нет, будет определено по типу поля

IN и NOT IN
```
{'SELECT': [], 'CALCULATION': [],
'WHERE': [{'field_name': 'game_name', 'where': 'NOT IN', 'condition': ['First Game', 'Second Game']}]}
```

BETWEEN
```
{'SELECT': [], 'CALCULATION': [],
'WHERE': [{'field_name': 'bk_game_id', 'where': 'BETWEEN', 'condition': ['1', '1000']}]}
```

LIKE
```
{'SELECT': [], 'CALCULATION': [],
'WHERE': [{'field_name': 'game_name', 'where': 'LIKE', 'condition': '%atma%'}]}
```

Остальное
```
{'SELECT': [], 'CALCULATION': [],
'WHERE': [{'field_name': 'game_name', 'where': '=', 'condition': 'Atom heart'}]}
```

### Получение данных для фильтрации, а именно Distinct
JSON для запроса
```
{'SELECT_DISTINCT': {'field_name': 'game_name', 'type': 'all/max-min/'}}
```
Процедура запуска осуществляется с помощью функции
```
OlapService.select_filter_for_frontend(OlapFilterFrontend, OlapTablesCollection)
```
