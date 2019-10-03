/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.oskar.analysis.variant.gwas;

import java.util.HashMap;
import java.util.Map;

public class GwasConfiguration {

    public enum Method {
        FISHER_TEST("fisher-test"),
        CHI_SQUARE_TEST("chi-square-test");

        public final String label;

        Method(String label) {
            this.label = label;
        }
    }

    public enum FisherMode {
        LESS("less", 1),
        GREATER("greater", 2),
        TWO_SIDED("two-sided", 3);

        private static final Map<String, FisherMode> BY_LABEL = new HashMap<>();
        private static final Map<Integer, FisherMode> BY_NUMBER = new HashMap<>();

        static {
            for (FisherMode e : values()) {
                BY_LABEL.put(e.label, e);
                BY_NUMBER.put(e.number, e);
            }
        }

        public final String label;
        public final int number;

        FisherMode(String label, int number) {
            this.label = label;
            this.number = number;
        }

        public static FisherMode valueOfLabel(String label) {
            return BY_LABEL.get(label);
        }

        public static FisherMode valueOfNumber(int number) {
            return BY_NUMBER.get(number);
        }
    }

    private Method method;
    private FisherMode fisherMode;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GwasConfiguration{");
        sb.append("method=").append(method);
        sb.append('}');
        return sb.toString();
    }

    public Method getMethod() {
        return method;
    }

    public GwasConfiguration setMethod(Method method) {
        this.method = method;
        return this;
    }

    public FisherMode getFisherMode() {
        return fisherMode;
    }

    public GwasConfiguration setFisherMode(FisherMode fisherMode) {
        this.fisherMode = fisherMode;
        return this;
    }
}
