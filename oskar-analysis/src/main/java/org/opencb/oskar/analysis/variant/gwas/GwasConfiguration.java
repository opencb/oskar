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

public class GwasConfiguration {

    public enum Method {
        FISHER_TEST,
        CHI_SQUARE_TEST,
        TDT
    }

    private Method method;

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
}