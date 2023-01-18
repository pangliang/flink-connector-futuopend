# flink-connector-futuopend

## Usage for Table/SQL API

We need several steps to setup a Flink cluster with the provided connector.

1. Setup a Flink cluster with version 1.14+ and Java 8+ installed.
2. Download the connector SQL jars from the Download page (or build yourself).
3. Put the downloaded jars under FLINK_HOME/lib/.
4. Restart the Flink cluster.

The example shows how to create a table using the connector.
```sql
-- create a table source with json format
CREATE TEMPORARY TABLE basic_qot (
  security row<code string, market int>,
  openPrice double,
  lowPrice double,
  highPrice double,
  curPrice double,
  volume double,
  updateTime string
)
WITH (
  'subType'='Basic',
  'codes' = 'HK|00700,HSI2302',
  'connector' = 'FutuOpenD',
  'hostname' = 'futu-opend.default',
  'port' = '11111',
  'format' = 'json'
);

-- read update basic quote data
select *  from basic_qot;
```

Or we can also use the raw format to simply view the data structure of each subType
```sql
CREATE TEMPORARY TABLE basic_qot (
  data string
)
WITH (
  'subType'='Basic',
  'codes' = 'HK|00700,HSI2302',
  'connector' = 'FutuOpenD',
  'hostname' = 'futu-opend.default',
  'port' = '11111',
  'format' = 'raw'
);
```

## Connector Options

| Option | Required | Default                | Description                                                                 |
| :--- | :--- |:-----------------------|:----------------------------------------------------------------------------|
| connector | required | (none)                 | Specify what connector to use, here should be 'FutuOpenD'.                  |
| hostname | required | 127.0.0.1              | The hostname of the FutuOpenD server.                                       |
| port | required | 11111                  | The port of the FutuOpenD server.                                           |
| subType | required | Basic                  | The subType of the data source. *(1)                                        |
| codes | required | HK&#124;00700,HSI2302  | The codes of the data source. *(2)                                          |
| format | required | json                   | The format of the data source. Currently only supports json and raw formats |

### *(1) subType

* Value refer to enum `SubType` in the [FutuOpenD Api #subType](https://futunnopen.github.io/futu-api-doc/protocol/protocol.html#sub-type).
and removes the prefix `SubType_`. e.g. `SubType_Basic` -> `Basic`, `SubType_OrderBook` -> `OrderBook`, `SubType_KL_5Min` -> `KL_5Min`

### *(2) codes

* Same markets codes separated by commas, e.g. `HK|00700,HSI2302`. 
* Different markets codes separated by semicolons, e.g. `HK|00700,HSI2302;US|AAPL,BA`.

