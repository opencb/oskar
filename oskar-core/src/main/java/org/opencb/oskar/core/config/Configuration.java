/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.oskar.core.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 30/04/15.
 */
@JsonIgnoreProperties({"storageEngine", "studyMetadataManager"})
public class Configuration {

    private String defaultStorageEngineId;
    private String logLevel;
    private String logFile;

    private String toolFolder;

    private CellBaseConfiguration cellbase;
    private ServerConfiguration server;
    private CacheConfiguration cache;
    private SearchConfiguration search;

    private BenchmarkConfiguration benchmark;
    private List<StorageEngineConfiguration> storageEngines;

    protected static Logger logger = LoggerFactory.getLogger(Configuration.class);

    public Configuration() {
        this("", new ArrayList<>());
    }

    public Configuration(String defaultStorageEngineId, List<StorageEngineConfiguration> storageEngines) {
        this.defaultStorageEngineId = defaultStorageEngineId;
        this.storageEngines = storageEngines;

        this.cellbase = new CellBaseConfiguration();
        this.server = new ServerConfiguration();
        this.cache = new CacheConfiguration();
        this.search = new SearchConfiguration();
    }

//    /*
//     * This method attempts to find and load the configuration from installation directory,
//     * if not exists then loads JAR storage-configuration.yml.
//     *
//     * @throws IOException If any IO problem occurs
//     */
//    @Deprecated
//    public static StorageConfiguration load() throws IOException {
//        String appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
//        Path path = Paths.get(appHome + "/conf/storage-configuration.yml");
//        if (appHome != null && Files.exists(path)) {
//            logger.debug("Loading configuration from '{}'", appHome + "/conf/storage-configuration.yml");
//            return StorageConfiguration
//                    .load(new FileInputStream(new File(appHome + "/conf/storage-configuration.yml")));
//        } else {
//            logger.debug("Loading configuration from '{}'",
//                    StorageConfiguration.class.getClassLoader()
//                            .getResourceAsStream("storage-configuration.yml")
//                            .toString());
//            return StorageConfiguration
//                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
//        }
//    }

    public static Configuration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static Configuration load(InputStream configurationInputStream, String format) throws IOException {
        Configuration configuration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
                break;
        }

        return configuration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    public StorageEngineConfiguration getStorageEngine() {
        return getStorageEngine(defaultStorageEngineId);
    }

    public StorageEngineConfiguration getStorageEngine(String storageEngine) {
        StorageEngineConfiguration storageEngineConfiguration = null;
        for (StorageEngineConfiguration engine : storageEngines) {
            if (engine.getId().equals(storageEngine)) {
                storageEngineConfiguration = engine;
                break;
            }
        }

        if (storageEngineConfiguration == null && storageEngines.size() > 0) {
            storageEngineConfiguration = storageEngines.get(0);
        }

        return storageEngineConfiguration;
    }

    public void addStorageEngine(InputStream configurationInputStream) throws IOException {
        addStorageEngine(configurationInputStream, "yaml");
    }

    public void addStorageEngine(InputStream configurationInputStream, String format) throws IOException {
        StorageEngineConfiguration storageEngineConfiguration = null;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                storageEngineConfiguration = objectMapper.readValue(configurationInputStream, StorageEngineConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                storageEngineConfiguration = objectMapper.readValue(configurationInputStream, StorageEngineConfiguration.class);
                break;
        }

        if (storageEngineConfiguration != null) {
            this.storageEngines.add(storageEngineConfiguration);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("defaultStorageEngineId='").append(defaultStorageEngineId).append('\'');
        sb.append(", logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", toolFolder='").append(toolFolder).append('\'');
        sb.append(", cellbase=").append(cellbase);
        sb.append(", server=").append(server);
        sb.append(", cache=").append(cache);
        sb.append(", search=").append(search);
        sb.append(", benchmark=").append(benchmark);
        sb.append(", storageEngines=").append(storageEngines);
        sb.append('}');
        return sb.toString();
    }

    public String getDefaultStorageEngineId() {
        return defaultStorageEngineId;
    }

    public void setDefaultStorageEngineId(String defaultStorageEngineId) {
        this.defaultStorageEngineId = defaultStorageEngineId;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getToolFolder() {
        return toolFolder;
    }

    public Configuration setToolFolder(String toolFolder) {
        this.toolFolder = toolFolder;
        return this;
    }

    public CellBaseConfiguration getCellbase() {
        return cellbase;
    }

    public void setCellbase(CellBaseConfiguration cellbase) {
        this.cellbase = cellbase;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public void setServer(ServerConfiguration server) {
        this.server = server;
    }

    public CacheConfiguration getCache() {
        return cache;
    }

    public Configuration setCache(CacheConfiguration cache) {
        this.cache = cache;
        return this;
    }

    public SearchConfiguration getSearch() {
        return search;
    }

    public Configuration setSearch(SearchConfiguration search) {
        this.search = search;
        return this;
    }

    public BenchmarkConfiguration getBenchmark() {
        return benchmark;
    }

    public void setBenchmark(BenchmarkConfiguration benchmark) {
        this.benchmark = benchmark;
    }

    public List<StorageEngineConfiguration> getStorageEngines() {
        return storageEngines;
    }

    public void setStorageEngines(List<StorageEngineConfiguration> storageEngines) {
        this.storageEngines = storageEngines;
    }

}
