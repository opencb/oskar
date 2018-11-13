package org.opencb.oskar.spark.variant.analysis.wrappers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.analysis.exceptions.AnalysisToolException;
import org.opencb.oskar.analysis.executor.Executor;
import org.opencb.oskar.core.config.OskarConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class PlinkWrapper extends VariantAnalysisWrapper {
    public static final String ANALYSIS_NAME = "plink";

    private String inFilename;
    private String metaFilename;
    private Query query;
    private Map<String, String> plinkParams;

    private Logger logger;

    public PlinkWrapper(String studyId, String inFilename, String metaFilename,
                        Query query, Map<String, String> plinkParams, OskarConfiguration configuration) {
        super(studyId, configuration);
        this.inFilename = inFilename;
        this.metaFilename = metaFilename;
        this.query = query;
        this.plinkParams = plinkParams;

        this.logger = LoggerFactory.getLogger(PlinkWrapper.class);
    }


    public void execute() throws AnalysisExecutorException {
        // Sanity check
        Path binPath;
        try {
            binPath = Paths.get(configuration.getAnalysis().get(ANALYSIS_NAME).getPath());
            if (binPath == null || !binPath.toFile().exists()) {
                String msg = "PLINK binary path is missing or does not exist:  '" + binPath + "'.";
                logger.error(msg);
                throw new AnalysisExecutorException(msg);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e.getMessage());
        }

        // Get output dir
        Path outDir = Paths.get("/tmp");
        if (plinkParams.get("out") != null) {
            outDir = Paths.get(plinkParams.get("out")).getParent();
        }

        // Generate VCF file by calling VCF exporter from query and query options
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(metaFilename));

            SparkSession sparkSession = SparkSession.builder().appName("variant-plink").getOrCreate();

//            VariantDataset vd = new VariantDataset(sparkSession);
//            vd.load(inFilename);
//            vd.createOrReplaceTempView("vcf");
//
//            if (query != null) {
//                vd.setQuery(query);
//            }

            // out filename
            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
//            exportPedMapFile(vd, studyMetadata, outDir + "/plink");

            // close
            sparkSession.stop();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error executing PLINK tool when retrieving variants to PED and MAP files: {}", e.getMessage());
            return;
        }

        // Execute PLINK
        StringBuilder sb = new StringBuilder();
        sb.append(binPath);
        sb.append(" --file ").append(outDir).append("/plink");
        for (String key : plinkParams.keySet()) {
            sb.append(" --").append(key);
            String value = plinkParams.get(key);
            if (!StringUtils.isEmpty(value)) {
                sb.append(" ").append(value);
            }
        }
        try {
            Executor.execute(sb.toString(), outDir, true);
        } catch (AnalysisToolException e) {
            logger.error(e.getMessage());
            throw new AnalysisExecutorException(e);
        }
    }

//    public void exportPedMapFile(VariantDataset variantDataset, VariantStudyMetadata studyMetadata,
//                                 String prefix) throws FileNotFoundException {
//        Path pedPath = Paths.get(prefix + ".ped");
//        Path mapPath = Paths.get(prefix + ".map");
//
//        StringBuilder sb = new StringBuilder();
//        PrintWriter pedWriter = new PrintWriter(pedPath.toFile());
//        PrintWriter mapWriter = new PrintWriter(mapPath.toFile());
//        Iterator<Variant> iterator = variantDataset.iterator();
//
//        List<String> sampleNames = VariantMetadataUtils.getSampleNames(studyMetadata);
//        //List<String> ms = new ArrayList<>(sampleNames.size());
//        StringBuilder[] markers = new StringBuilder[sampleNames.size()];
//        while (iterator.hasNext()) {
//            Variant variant = iterator.next();
//            // genotypes
//            List<List<String>> sampleData = variant.getStudiesMap().get(studyMetadata.getId()).getSamplesData();
//            assert(sampleData.size() == sampleNames.size());
//            for (int i = 0; i < sampleData.size(); i++) {
//                String[] gt = sampleData.get(i).get(0).split("[|/]");
//                if (markers[i] == null) {
//                    markers[i] = new StringBuilder();
//                }
//                markers[i].append("\t"
//                        + (gt[0].equals("1") ? variant.getAlternate() : variant.getReference())
//                        + "\t"
//                        + (gt[1].equals("1") ? variant.getAlternate() : variant.getReference()));
//            }
//
//            // map file line
//            mapWriter.println(variant.getChromosome() + "\t" + variant.getId() + "\t0\t" + variant.getStart());
//        }
//
//        // ped file line
//        for (int i = 0; i < sampleNames.size(); i++) {
//            sb.setLength(0);
//            String sampleName = sampleNames.get(i);
//            Individual individual = getIndividualBySampleName(sampleName, studyMetadata);
//            if (individual == null) {
//                // sample not found, what to do??
//                sb.append(0).append("\t");
//                sb.append(sampleName).append("\t");
//                sb.append(0).append("\t");
//                sb.append(0).append("\t");
//                sb.append(0).append("\t");
//                sb.append(0);
//            } else {
//                int sex = org.opencb.biodata.models.core.pedigrees.Individual.Sex
//                        .getEnum(individual.getSex()).getValue();
//                int phenotype = org.opencb.biodata.models.core.pedigrees.Individual.AffectionStatus
//                        .getEnum(individual.getPhenotype()).getValue();
//                sb.append(individual.getFamily() == null ? 0 : individual.getFamily()).append("\t");
//                sb.append(sampleName).append("\t");
//                sb.append(individual.getFather() == null ? 0 : individual.getFather()).append("\t");
//                sb.append(individual.getMother() == null ? 0 : individual.getMother()).append("\t");
//                sb.append(sex).append("\t");
//                sb.append(phenotype);
//            }
//            sb.append(markers[i]);
//            pedWriter.println(sb.toString());
//        }
//
//        // close
//        pedWriter.close();
//        mapWriter.close();
//    }

    private Individual getIndividualBySampleName(String sampleName, VariantStudyMetadata studyMetadata) {
        for (Individual individual: studyMetadata.getIndividuals()) {
            for (Sample sample: individual.getSamples()) {
                if (sampleName.equals(sample.getId())) {
                    return individual;
                }
            }
        }
        return null;
    }
}
