package com.zhaoxiaodan.flink.futuopend;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

@Slf4j
public class FutuOpenDDynamicTableSource implements ScanTableSource {
    private final FutuOpenDConfig futuOpenDConfig;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public FutuOpenDDynamicTableSource(FutuOpenDConfig futuOpenDConfig, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, DataType producedDataType) {
        this.futuOpenDConfig = futuOpenDConfig;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
            runtimeProviderContext,
            producedDataType
        );
        final SourceFunction<RowData> sourceFunction = new FutuOpenDSourceFunction(
            futuOpenDConfig,
            deserializer
        );
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new FutuOpenDDynamicTableSource(futuOpenDConfig, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "FutuOpenD Table Source";
    }
}
