package io.cobble.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/** Exact format-id and optional external-path resolver for built-in and service-loaded plugins. */
public final class TableFormatPluginRegistry {
    private TableFormatPluginRegistry() {}

    /** Resolves one exact format id in the current class loader. */
    public static TableFormatPlugin resolve(String formatId) {
        return fromPlugins(loadedPlugins()).resolve(formatId);
    }

    /** Resolves exactly one path; plugins must not return an empty plan. */
    public static TableReadSnapshot resolvePath(io.cobble.Config config, TablePathRequest request)
            throws Exception {
        return fromPlugins(loadedPlugins()).resolvePath(config, request);
    }

    static Registry fromPlugins(Collection<? extends TableFormatPlugin> plugins) {
        return new Registry(plugins);
    }

    private static Collection<TableFormatPlugin> loadedPlugins() {
        ArrayList<TableFormatPlugin> plugins = new ArrayList<TableFormatPlugin>();
        for (TableFormatPlugin plugin : ServiceLoader.load(TableFormatPlugin.class)) {
            plugins.add(plugin);
        }
        // Native layout is a fallback after format-specific external layouts have declined.
        plugins.add(new NativeTableFormatPlugin());
        return plugins;
    }

    static final class Registry {
        private final Map<String, TableFormatPlugin> plugins;

        private Registry(Collection<? extends TableFormatPlugin> values) {
            if (values == null) throw new IllegalArgumentException("plugins must not be null");
            plugins = new LinkedHashMap<String, TableFormatPlugin>();
            for (TableFormatPlugin plugin : values) {
                if (plugin == null)
                    throw new IllegalArgumentException("table format plugin must not be null");
                String formatId = requireText(plugin.formatId(), "table format plugin id");
                TableFormatPlugin previous = plugins.put(formatId, plugin);
                if (previous != null) {
                    throw new IllegalStateException(
                            "duplicate table format id '"
                                    + formatId
                                    + "' from "
                                    + previous.getClass().getName()
                                    + " and "
                                    + plugin.getClass().getName());
                }
            }
        }

        TableFormatPlugin resolve(String formatId) {
            TableFormatPlugin plugin = plugins.get(requireText(formatId, "snapshot format id"));
            if (plugin == null) {
                throw new IllegalArgumentException(
                        "no table format plugin is installed for snapshot format id '"
                                + formatId
                                + "'");
            }
            return plugin;
        }

        TableReadSnapshot resolvePath(io.cobble.Config config, TablePathRequest request)
                throws Exception {
            if (config == null) throw new IllegalArgumentException("config must not be null");
            if (request == null) throw new IllegalArgumentException("request must not be null");
            TableReadSnapshot resolved = null;
            String resolvedBy = null;
            TableFormatPlugin nativeFallback = null;
            for (TableFormatPlugin plugin : plugins.values()) {
                if (plugin instanceof NativeTableFormatPlugin) {
                    nativeFallback = plugin;
                    continue;
                }
                Optional<TableReadSnapshot> candidate = plugin.resolvePath(config, request);
                if (candidate == null) {
                    throw new IllegalStateException(
                            "table format plugin "
                                    + plugin.getClass().getName()
                                    + " returned null Optional");
                }
                if (!candidate.isPresent()) continue;
                if (resolved != null) {
                    throw new IllegalStateException(
                            "multiple table format plugins accepted '"
                                    + request.path()
                                    + "': "
                                    + resolvedBy
                                    + " and "
                                    + plugin.getClass().getName());
                }
                resolved = candidate.get();
                resolvedBy = plugin.getClass().getName();
            }
            if (resolved == null && nativeFallback != null) {
                Optional<TableReadSnapshot> candidate = nativeFallback.resolvePath(config, request);
                if (candidate == null) {
                    throw new IllegalStateException(
                            "native table format plugin returned null Optional");
                }
                if (candidate.isPresent()) {
                    resolved = candidate.get();
                    resolvedBy = nativeFallback.getClass().getName();
                }
            }
            if (resolved == null) {
                throw new IllegalArgumentException(
                        "no table format plugin accepted '"
                                + request.path()
                                + "'; install the plugin for this layout");
            }
            return resolved;
        }

        private static String requireText(String value, String name) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(name + " must not be empty");
            }
            return value;
        }
    }
}
