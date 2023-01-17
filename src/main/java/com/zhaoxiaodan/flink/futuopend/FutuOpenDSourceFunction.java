package com.zhaoxiaodan.flink.futuopend;

import java.util.concurrent.CountDownLatch;

import com.futu.openapi.FTAPI;
import com.futu.openapi.FTAPI_Conn;
import com.futu.openapi.FTAPI_Conn_Qot;
import com.futu.openapi.FTSPI_Conn;
import com.futu.openapi.FTSPI_Qot;
import com.futu.openapi.pb.QotCommon;
import com.futu.openapi.pb.QotCommon.QotMarket;
import com.futu.openapi.pb.QotCommon.SubType;
import com.futu.openapi.pb.QotSub;
import com.futu.openapi.pb.QotSub.Response;
import com.futu.openapi.pb.QotUpdateBasicQot;
import com.futu.openapi.pb.QotUpdateBroker;
import com.futu.openapi.pb.QotUpdateKL;
import com.futu.openapi.pb.QotUpdateOrderBook;
import com.futu.openapi.pb.QotUpdateRT;
import com.futu.openapi.pb.QotUpdateTicker;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

@Slf4j
public class FutuOpenDSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData>, FTSPI_Qot, FTSPI_Conn {
    private final FutuOpenDConfig futuOpenDConfig;
    private final DeserializationSchema<RowData> deserializer;
    private volatile boolean isRunning = true;
    private FTAPI_Conn_Qot qot;
    private CountDownLatch connectLock;
    private SourceContext<RowData> ctx;

    public FutuOpenDSourceFunction(FutuOpenDConfig futuOpenDConfig, DeserializationSchema<RowData> deserializer) {
        this.futuOpenDConfig = futuOpenDConfig;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));

        FTAPI.init();
        this.qot = new FTAPI_Conn_Qot();
        this.connectLock = new CountDownLatch(1);

        qot.setClientInfo("flink-connector-futuopend", 1);
        qot.setConnSpi(this);
        qot.setQotSpi(this);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        this.ctx = ctx;

        qot.initConnect(futuOpenDConfig.getOpendIP(), futuOpenDConfig.getOpendPort().shortValue(), false);
        connectLock.await();

        SubType subType = SubType.valueOf("SubType_" + futuOpenDConfig.getSubType());

        for (String marketGroups : futuOpenDConfig.getCodes().split(";")) {
            String[] parts = marketGroups.split("\\|");
            if(parts.length != 2) {
                throw new IllegalArgumentException("Invalid marketGroups: " + marketGroups + ", should be like: HK|00700,00701;US|AAPL,GOOG");
            }
            String marketStr = parts[0];
            String codesStr = parts[1];
            for(String code : codesStr.split(",")) {
                QotMarket qotMarket = QotMarket.valueOf("QotMarket_" + marketStr + "_Security");
                QotCommon.Security sec = QotCommon.Security.newBuilder()
                    .setCode(code)
                    .setMarket(qotMarket.getNumber())
                    .build();
                QotSub.C2S c2s = QotSub.C2S.newBuilder()
                    .addSecurityList(sec)
                    .addSubTypeList(subType.getNumber())
                    .setIsSubOrUnSub(true)
                    .setIsRegOrUnRegPush(true)
                    .setIsFirstPush(true)
                    .build();
                QotSub.Request req = QotSub.Request.newBuilder().setC2S(c2s).build();
                int seqNo = qot.sub(req);
                log.info("sub seqNo: {}", seqNo);
            }
        }

        while (isRunning) {
            Thread.sleep(Long.MAX_VALUE);
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
    }

    @Override
    public void onPush_UpdateBasicQuote(FTAPI_Conn client, QotUpdateBasicQot.Response rsp) {
        log.debug("onReply_GetBasicQot: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C().getBasicQotListList());
        if (rsp.getRetType() != 0 || CollectionUtils.isEmpty(rsp.getS2C().getBasicQotListList())) {
            return;
        }

        rsp.getS2C().getBasicQotListList().forEach(basicQot -> {
            try{
                RowData rowData = deserializer.deserialize(JsonFormat.printer().print(basicQot).getBytes());
                ctx.collect(rowData);
            } catch (Exception e) {
                log.error("deserialize error", e);
            }
        });
    }

    @Override
    public void onPush_UpdateKL(FTAPI_Conn client, QotUpdateKL.Response rsp) {
        log.debug("onPush_UpdateKL: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C());

        try{
            RowData rowData = deserializer.deserialize(JsonFormat.printer().print(rsp.getS2C()).getBytes());
            ctx.collect(rowData);
        } catch (Exception e) {
            log.error("deserialize error", e);
        }
    }

    @Override
    public void onPush_UpdateOrderBook(FTAPI_Conn client, QotUpdateOrderBook.Response rsp) {
        log.debug("onPush_UpdateOrderBook: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C());

        try{
            RowData rowData = deserializer.deserialize(JsonFormat.printer().print(rsp.getS2C()).getBytes());
            ctx.collect(rowData);
        } catch (Exception e) {
            log.error("deserialize error", e);
        }
    }

    @Override
    public void onPush_UpdateRT(FTAPI_Conn client, QotUpdateRT.Response rsp) {
        log.debug("onPush_UpdateRT: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C());
        try{
            RowData rowData = deserializer.deserialize(JsonFormat.printer().print(rsp.getS2C()).getBytes());
            ctx.collect(rowData);
        } catch (Exception e) {
            log.error("deserialize error", e);
        }
    }

    @Override
    public void onPush_UpdateTicker(FTAPI_Conn client, QotUpdateTicker.Response rsp) {
        log.debug("onPush_UpdateTicker: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C());
        try{
            RowData rowData = deserializer.deserialize(JsonFormat.printer().print(rsp.getS2C()).getBytes());
            ctx.collect(rowData);
        } catch (Exception e) {
            log.error("deserialize error", e);
        }
    }

    @Override
    public void onPush_UpdateBroker(FTAPI_Conn client, QotUpdateBroker.Response rsp) {
        log.debug("onPush_UpdateBroker: {}, {}, {}, {}", client.getConnectID(), rsp.getRetType(), rsp.getRetMsg(), rsp.getS2C());
        try{
            RowData rowData = deserializer.deserialize(JsonFormat.printer().print(rsp.getS2C()).getBytes());
            ctx.collect(rowData);
        } catch (Exception e) {
            log.error("deserialize error", e);
        }
    }
}
