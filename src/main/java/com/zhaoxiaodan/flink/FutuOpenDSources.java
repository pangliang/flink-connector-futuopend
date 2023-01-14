package com.zhaoxiaodan.flink;

import com.futu.openapi.*;
import com.futu.openapi.pb.QotCommon;
import com.futu.openapi.pb.QotCommon.BasicQot;
import com.futu.openapi.pb.QotCommon.QotMarket;
import com.futu.openapi.pb.QotSub;
import com.futu.openapi.pb.QotSub.Response;
import com.futu.openapi.pb.QotUpdateBasicQot;
import com.futu.openapi.pb.QotUpdateKL;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author pangliang
 * @date 2023/01/15
 */
@Slf4j
public class FutuOpenDSources {
    public static void main(String[] args) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        final TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql("CREATE TABLE basic_qot (`security.code` STRING, curPrice DOUBLE, updateTimestamp DOUBLE) WITH ("
            + "  'connector'='FutuOpenD'"
            + "  ,'hostname'='127.0.0.1'"
            + "  ,'port'='11111'"
            + "  ,'codes'='HK|00700'"
            + ")");

        Table rs = tableEnv.sqlQuery("select *, cast(updateTimestamp as bigint) from basic_qot");
        rs.execute().print();

        while (true) {
            Thread.sleep(1000);
            log.info("sleep");
        }
    }

    public static class FutuOpenDDynamicTableFactory implements DynamicTableSourceFactory {
        private static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .defaultValue("127.0.0.1");
        private static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .defaultValue(11111);
        private static final ConfigOption<String> CODES = ConfigOptions.key("codes")
            .stringType()
            .noDefaultValue();

        @Override
        public String factoryIdentifier() {
            return "FutuOpenD"; // used for matching to `connector = '...'`
        }
        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.add(HOSTNAME);
            options.add(PORT);
            options.add(CODES);
            return options;
        }
        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }
        @Override
        public DynamicTableSource createDynamicTableSource(Context context) {
            // either implement your custom validation logic here ...
            // or use the provided helper utility
            final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
            helper.validate();

            final ReadableConfig options = helper.getOptions();

            FutuOpenDConfig futuOpenDConfig = new FutuOpenDConfig();
            futuOpenDConfig.setOpendIP(options.get(HOSTNAME));
            futuOpenDConfig.setOpendPort(options.get(PORT));
            futuOpenDConfig.setCodes(options.get(CODES));

            final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
            return new FutuOpenDDynamicTableSource(futuOpenDConfig, producedDataType);
        }
    }

    public static class FutuOpenDDynamicTableSource implements ScanTableSource {
        private final FutuOpenDConfig futuOpenDConfig;
        private final DataType producedDataType;
        public FutuOpenDDynamicTableSource(FutuOpenDConfig futuOpenDConfig, DataType producedDataType) {
            this.futuOpenDConfig = futuOpenDConfig;
            this.producedDataType = producedDataType;
        }
        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
        }
        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            final RowType rowType = (RowType) producedDataType.getLogicalType();
            final TypeInformation<RowData> rowDataTypeInfo = runtimeProviderContext.createTypeInformation(producedDataType);
            final SourceFunction<RowData> sourceFunction = new FutuOpenDSourceFunction(
                futuOpenDConfig,
                rowType,
                rowDataTypeInfo
            );
            return SourceFunctionProvider.of(sourceFunction, false);
        }
        @Override
        public DynamicTableSource copy() {
            return new FutuOpenDDynamicTableSource(futuOpenDConfig, producedDataType);
        }
        @Override
        public String asSummaryString() {
            return "Socket Table Source";
        }
    }

    public static class FutuOpenDSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData>, FTSPI_Qot, FTSPI_Conn {
        private final FutuOpenDConfig futuOpenDConfig;
        private final RowType rowType;
        private final TypeInformation<RowData> resultTypeInfo;
        private final LinkedBlockingQueue<BasicQot> basicQotQueue = new LinkedBlockingQueue<>(100);
        private volatile boolean isRunning = true;
        private FTAPI_Conn_Qot qot;
        private CountDownLatch connectLock;

        public FutuOpenDSourceFunction(FutuOpenDConfig futuOpenDConfig, RowType rowType, TypeInformation<RowData> resultTypeInfo) {
            this.futuOpenDConfig = futuOpenDConfig;
            this.rowType = rowType;
            this.resultTypeInfo = resultTypeInfo;
        }
        @Override
        public TypeInformation<RowData> getProducedType() {
            return resultTypeInfo;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            FTAPI.init();
            this.qot = new FTAPI_Conn_Qot();
            this.connectLock = new CountDownLatch(1);

            qot.setClientInfo("javaclient", 1);
            qot.setConnSpi(this);
            qot.setQotSpi(this);
        }
        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            qot.initConnect(futuOpenDConfig.opendIP, futuOpenDConfig.opendPort.shortValue(), false);
            connectLock.await();

            for(String code : futuOpenDConfig.codes.split(",")) {
                int market = QotMarket.QotMarket_HK_Security_VALUE;
                String[] parts = code.split("\\|");
                switch (parts[0].toUpperCase()) {
                    case "HK":
                        market = QotMarket.QotMarket_HK_Security_VALUE;
                        break;
                    case "SH":
                        market = QotMarket.QotMarket_CNSH_Security_VALUE;
                        break;
                    case "US":
                        market = QotMarket.QotMarket_US_Security_VALUE;
                        break;
                }
                QotCommon.Security sec = QotCommon.Security.newBuilder()
                        .setCode("00700")
                        .setMarket(market)
                        .build();
                QotSub.C2S c2s = QotSub.C2S.newBuilder()
                        .addSecurityList(sec)
                        .addSubTypeList(QotCommon.SubType.SubType_KL_1Min_VALUE)
                        .setIsSubOrUnSub(true)
                        .setIsFirstPush(true)
                        .build();
                QotSub.Request req = QotSub.Request.newBuilder().setC2S(c2s).build();
                int seqNo = qot.sub(req);
                log.info("sub seqNo: {}", seqNo);
            }

            while (isRunning) {
                BasicQot basicQot = this.basicQotQueue.poll(1000, TimeUnit.MILLISECONDS);
                if(null == basicQot) {
                    continue;
                }

                List<RowField> rowFields = rowType.getFields();
                GenericRowData rowData = new GenericRowData(rowFields.size());

                int i = 0;
                for(RowField rowField: rowFields) {
                    String fieldName = rowField.getName();
                    switch (rowField.getType().getTypeRoot()) {
                        case VARCHAR:
                            Object strValue = PropertyUtils.getProperty(basicQot, fieldName);
                            rowData.setField(i, StringData.fromString(strValue == null ? "null" : (String)strValue));
                            break;
                        default:
                            rowData.setField(i, PropertyUtils.getProperty(basicQot, fieldName));
                    }

                    i++;
                }

                ctx.collect(rowData);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void onInitConnect(FTAPI_Conn client, long errCode, String desc) {
            log.info("onInitConnect: {}, {}, {}", client.getConnectID(), errCode, desc);
            connectLock.countDown();
        }

        @Override
        public void onReply_Sub(FTAPI_Conn client, int nSerialNo, Response rsp) {
            log.info("onReply_Sub: {}, {}, {}, {}, {}", client.getConnectID(), nSerialNo, rsp.getRetType(), rsp.getRetMsg(), rsp.getErrCode());
            if(rsp.getRetType() != 0) {
                this.cancel();
            }
        }

        @Override
        public void onPush_UpdateBasicQuote(FTAPI_Conn client, QotUpdateBasicQot.Response rsp) {
            log.info("onReply_GetBasicQot: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C().getBasicQotListList());
            if(rsp.getRetType() == 0 && rsp.getS2C() != null && CollectionUtils.isNotEmpty(rsp.getS2C().getBasicQotListList())) {
                rsp.getS2C().getBasicQotListList().forEach(basicQot -> {
                    try {
                        this.basicQotQueue.put(basicQot);
                    } catch (InterruptedException e) {
                        log.error("", e);
                    }
                });
            }
        }

        @Override
        public void onPush_UpdateKL(FTAPI_Conn client, QotUpdateKL.Response rsp) {
            log.info("onPush_UpdateKL: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C().getKlListList());
        }
    }

    @Data
    public static class FutuOpenDConfig implements Serializable {
        private String opendIP = "127.0.0.1";
        private Integer opendPort = 11111;
        private String codes;
    }
}


