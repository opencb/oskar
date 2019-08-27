package org.opencb.oskar.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.AbstractAnalysisExecutor;

import java.util.List;

public abstract class AbstractGwasExecutor extends AbstractAnalysisExecutor {

    private List<String> list1;
    private List<String> list2;
    private ObjectMap executorParams;
    private GwasConfiguration configuration;

    public AbstractGwasExecutor() {
    }

    public AbstractGwasExecutor(List<String> list1, List<String> list2, ObjectMap params, GwasConfiguration configuration) {
        this.setup(list1, list2, params,  configuration);
    }

    public void setup(List<String> list1, List<String> list2, ObjectMap executorParams, GwasConfiguration configuration) {
        this.list1 = list1;
        this.list2 = list2;
        this.executorParams = executorParams;
        this.configuration = configuration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GwasExecutor{");
        sb.append("list1=").append(list1);
        sb.append(", list2=").append(list2);
        sb.append(", executorParams=").append(executorParams);
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getList1() {
        return list1;
    }

    public AbstractGwasExecutor setList1(List<String> list1) {
        this.list1 = list1;
        return this;
    }

    public List<String> getList2() {
        return list2;
    }

    public AbstractGwasExecutor setList2(List<String> list2) {
        this.list2 = list2;
        return this;
    }

    public ObjectMap getExecutorParams() {
        return executorParams;
    }

    public AbstractGwasExecutor setExecutorParams(ObjectMap executorParams) {
        this.executorParams = executorParams;
        return this;
    }

    public GwasConfiguration getConfiguration() {
        return configuration;
    }

    public AbstractGwasExecutor setConfiguration(GwasConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
