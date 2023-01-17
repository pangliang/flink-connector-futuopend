package com.zhaoxiaodan.flink.futuopend;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class FutuOpenDSourceFunctionTest {

    @Test
    public void basicQotTest() {
        final EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        final TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("CREATE TABLE basic_qot ("
            + "  `security` ROW<code STRING, market INT>"
            + "  , openPrice DOUBLE"
            + "  , lowPrice DOUBLE"
            + "  , highPrice DOUBLE"
            + "  , curPrice DOUBLE"
            + "  , updateTimestamp DOUBLE"
            + ") WITH ("
            + "  'connector'='FutuOpenD'"
            + "  ,'hostname'='127.0.0.1'"
            + "  ,'port'='11111'"
            + "  ,'subType'='Basic'"
            + "  ,'codes'='HK|00700,HSI2302'"
            + "  ,'format'='json'"
            + ")");

        Table rs = tableEnv.sqlQuery("select *, security.code, cast(updateTimestamp as bigint) from basic_qot");
        rs.execute().print();
    }

    @Test
    public void orderBookTest() {
        final EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        final TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("CREATE TABLE order_book ("
            + "  `security` ROW<code STRING, market INT>"
            + "  ,orderBookAskList ARRAY<ROW<price DOUBLE, volume BIGINT, orederCount INT>>"
            + ") WITH ("
            + "  'connector'='FutuOpenD'"
            + "  ,'hostname'='127.0.0.1'"
            + "  ,'port'='11111'"
            + "  ,'subType'='OrderBook'"
            + "  ,'codes'='HK|00700,HSI2302'"
            + "  ,'format'='json'"
            + ")");

        Table rs = tableEnv.sqlQuery("select * from order_book");
        rs.execute().print();
    }
}
