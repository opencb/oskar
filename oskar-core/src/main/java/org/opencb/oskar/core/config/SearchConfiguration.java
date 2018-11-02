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


/**
 * Created by wasim on 09/11/16.
 */
public class SearchConfiguration {

    private String host;
    @Deprecated
    private String collection;
    private String user;
    private String password;
    private boolean active;
    private int timeout;
    private int rows;

    public static final boolean DEFAULT_ACTVE = true;
    public static final String DEFAULT_HOST = "localhost:8983/solr/";
    public static final String DEFAULT_COLLECTION = "variants";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_USER = "";
    public static final int DEFAULT_TIMEOUT = 45000;
    public static final int DEFAULT_ROWS = 100000;

    public SearchConfiguration() {
        this(DEFAULT_HOST, DEFAULT_COLLECTION, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_ACTVE,
                DEFAULT_TIMEOUT, DEFAULT_ROWS);
    }

    public SearchConfiguration(String host, String collection, String user, String password, boolean active, int timeout, int rows) {
        this.host = host;
        this.collection = collection;
        this.user = user;
        this.password = password;
        this.active = active;
        this.timeout = timeout;
        this.rows = rows;
    }

    public String getHost() {
        return host;
    }

    public SearchConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    @Deprecated
    public String getCollection() {
        return collection;
    }

    @Deprecated
    public SearchConfiguration setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public String getUser() {
        return user;
    }

    public SearchConfiguration setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SearchConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }

    public boolean getActive() {
        return active;
    }

    public SearchConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getRows() {
        return rows;
    }

    public SearchConfiguration setRows(int rows) {
        this.rows = rows;
        return this;
    }

    @Override
    public String toString() {
        return "SearchConfiguration{"
                + "host='" + host + '\''
                + ", collection='" + collection + '\''
                + ", user='" + user + '\''
                + ", active='" + active + '\''
                + ", rows='" + rows + '\''
                + '}';
    }
}
