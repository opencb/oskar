package org.opencb.oskar.spark.variant.analysis.wrappers;

import org.opencb.commons.datastore.core.Query;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.analysis.exceptions.AnalysisToolException;
import org.opencb.oskar.core.config.OskarConfiguration;
import org.opencb.oskar.analysis.executor.Executor;
import org.opencb.oskar.spark.variant.analysis.VariantAnalysisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by joaquin on 1/19/17.
 */
public class RvTestsWrapper extends VariantAnalysisWrapper {

    public static final String ANALYSIS_NAME = "rvtests";

    private String inFilename;
    private String metaFilename;
    private Query query;
    private Map<String, String> rvtestsParams;

    private Logger logger;

    public RvTestsWrapper(String studyId, String inFilename, String metaFilename,
                          Query query, Map<String, String> rvTestsParams, OskarConfiguration configuration) {
        super(studyId, configuration);
        this.inFilename = inFilename;
        this.metaFilename = metaFilename;
        this.query = query;
        this.rvtestsParams = rvTestsParams;

        this.logger = LoggerFactory.getLogger(RvTestsWrapper.class);
    }

    @Override
    public void execute() throws AnalysisExecutorException {
        // Sanity check
        Path binPath;
        try {
            binPath = Paths.get(configuration.getAnalysis().get(ANALYSIS_NAME).getPath());
            if (binPath == null || !binPath.toFile().exists()) {
                String msg = "RvTests binary path is missing or does not exist:  '" + binPath + "'.";
                logger.error(msg);
                throw new AnalysisExecutorException(msg);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e.getMessage());
        }

        // Get output dir
        Path outDir = Paths.get("/tmp");
        if (rvtestsParams.get("out") != null) {
            outDir = Paths.get(rvtestsParams.get("out")).getParent();
        }

        String vcfFilename = outDir.toString() + "/tmp.vcf";
        String pedFilename = outDir.toString() + "/tmp.vcf.ped";
        try {
            // Export target variants to VCF file
            VariantAnalysisUtils.exportVCF(inFilename, metaFilename, query, vcfFilename);
            VariantAnalysisUtils.exportPedigree(metaFilename, studyId, pedFilename);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }

        StringBuilder sb = new StringBuilder();
/*
        // Compress vcf to bgz
        sb.setLength(0);
        sb.append(BGZIP_BIN).append(" ").append(vcfFilename);
        Executor.execute(sb.toString(), outDir, true);

        // ...create tabix index
        sb.setLength(0);
        sb.append(TABIX_BIN).append(" -p vcf ").append(vcfFilename).append(".gz");
        Executor.execute(sb.toString(), outDir, true);
*/
        // ...and finally, run rvtests
        sb.setLength(0);
        sb.append(binPath);
        sb.append(" --inVcf ").append(vcfFilename); //.append(".gz");
        sb.append(" --pheno ").append(pedFilename);
        for (String key: rvtestsParams.keySet()) {
            sb.append(" --").append(key).append(" ").append(rvtestsParams.get(key));
        }
        try {
            Executor.execute(sb.toString(), outDir, true);
        } catch (AnalysisToolException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }
    }
}
