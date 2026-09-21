package io.cobble.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Exact format-id resolver for built-in and service-loaded table read factories. */
public final class TableReadFormatRegistry {
    private TableReadFormatRegistry() {}

    /** Plans a read by resolving the format embedded in the selected fixed snapshot. */
    public static TableScanPlan plan(io.cobble.Config config, TableReadSnapshot snapshot)
            throws Exception {
        if (config == null) throw new IllegalArgumentException("config must not be null");
        if (snapshot == null) throw new IllegalArgumentException("snapshot must not be null");
        return resolve(snapshot.formatId()).plan(config, snapshot);
    }

    /** Resolves one exact format id in the current class loader. */
    public static TableReadFormatFactory resolve(String formatId) {
        return fromFactories(loadedFactories()).resolve(formatId);
    }

    static Registry fromFactories(Collection<? extends TableReadFormatFactory> factories) {
        return new Registry(factories);
    }

    private static Collection<TableReadFormatFactory> loadedFactories() {
        ArrayList<TableReadFormatFactory> factories = new ArrayList<TableReadFormatFactory>();
        factories.add(new NativeTableReadFormatFactory());
        for (TableReadFormatFactory factory : ServiceLoader.load(TableReadFormatFactory.class)) {
            factories.add(factory);
        }
        return factories;
    }

    static final class Registry {
        private final Map<String, TableReadFormatFactory> factories;

        private Registry(Collection<? extends TableReadFormatFactory> values) {
            if (values == null) throw new IllegalArgumentException("factories must not be null");
            factories = new LinkedHashMap<String, TableReadFormatFactory>();
            for (TableReadFormatFactory factory : values) {
                if (factory == null)
                    throw new IllegalArgumentException("format factory must not be null");
                String formatId = requireText(factory.formatId(), "format factory id");
                TableReadFormatFactory previous = factories.put(formatId, factory);
                if (previous != null) {
                    throw new IllegalStateException(
                            "duplicate table read format id '"
                                    + formatId
                                    + "' from "
                                    + previous.getClass().getName()
                                    + " and "
                                    + factory.getClass().getName());
                }
            }
        }

        TableReadFormatFactory resolve(String formatId) {
            TableReadFormatFactory factory =
                    factories.get(requireText(formatId, "snapshot format id"));
            if (factory == null) {
                throw new IllegalArgumentException(
                        "no table read format factory is installed for snapshot format id '"
                                + formatId
                                + "'");
            }
            return factory;
        }

        private static String requireText(String value, String name) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(name + " must not be empty");
            }
            return value;
        }
    }
}
