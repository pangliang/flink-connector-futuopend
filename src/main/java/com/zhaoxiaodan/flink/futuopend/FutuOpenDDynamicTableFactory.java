package com.zhaoxiaodan.flink.futuopend;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

@Slf4j
public class FutuOpenDDynamicTableFactory implements DynamicTableSourceFactory {
    private static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
        .stringType()
        .defaultValue("127.0.0.1");
    private static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
        .intType()
        .defaultValue(11111);
    private static final ConfigOption<String> CODES = ConfigOptions.key("codes")
        .stringType()
        .defaultValue("HK|00700,HSI2302");
    private static final ConfigOption<String> SUBTYPE = ConfigOptions.key("subType")
        .stringType()
        .defaultValue("Basic");

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
        options.add(SUBTYPE);
        options.add(FactoryUtil.FORMAT);
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

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
            DeserializationFormatFactory.class,
            FactoryUtil.FORMAT
        );

        // validate all options
        helper.validate();

        final ReadableConfig options = helper.getOptions();

        FutuOpenDConfig futuOpenDConfig = new FutuOpenDConfig();
        futuOpenDConfig.setOpendIP(options.get(HOSTNAME));
        futuOpenDConfig.setOpendPort(options.get(PORT));
        futuOpenDConfig.setCodes(options.get(CODES));
        futuOpenDConfig.setSubType(options.get(SUBTYPE));

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new FutuOpenDDynamicTableSource(futuOpenDConfig, decodingFormat, producedDataType);
    }
}
