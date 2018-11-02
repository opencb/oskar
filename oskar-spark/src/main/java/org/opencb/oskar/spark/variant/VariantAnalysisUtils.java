package org.opencb.oskar.spark.variant;

import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.formats.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.converters.VCFExporter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class VariantAnalysisUtils {

    /**
     * Export Avro variant into a VCF file.
     *
     * @param inputAvroFilename Avro filename
     * @param metaFilename      Metadata filename
     * @param query             Query to filter variants
     * @param vcfFilename       Output VCF filename
     * @throws IOException      IOException
     */
    public static void exportVCF(String inputAvroFilename, String metaFilename, Query query, String vcfFilename) throws IOException {
        // Generate VCF file by calling VCF exporter from query and query options
        VariantMetadataManager manager = new VariantMetadataManager();
        manager.load(Paths.get(metaFilename));

        SparkSession sparkSession = SparkSession.builder().appName("vcf-exporter").getOrCreate();

//        VariantDataset vd = new VariantDataset(sparkSession);
//        vd.load(inputAvroFilename);
//        vd.createOrReplaceTempView("vcf");
//
//        // Add filters to variant dataset
//        if (query != null) {
//            vd.setQuery(query);
//        }

        // Export to VCF file
        VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
        VCFExporter vcfExporter = new VCFExporter(studyMetadata);
        vcfExporter.open(Paths.get(vcfFilename));

//        vcfExporter.export(vd.iterator());

        // Close everything
        vcfExporter.close();
        sparkSession.stop();
    }

    /**
     * Export variant metadata into pedigree file format.
     *
     * @param metaFilename  Variant metadata file name
     * @param studyId       Study ID target
     * @param pedFilename   Pedigree file name
     * @throws IOException  IO exception
     */
    public static void exportPedigree(String metaFilename, String studyId, String pedFilename) throws IOException {
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(Paths.get(metaFilename));
        List<Pedigree> pedigrees = metadataManager.getPedigree(studyId);
        PedigreeManager pedigreeManager = new PedigreeManager();
        pedigreeManager.save(pedigrees, Paths.get(pedFilename));
    }
}
