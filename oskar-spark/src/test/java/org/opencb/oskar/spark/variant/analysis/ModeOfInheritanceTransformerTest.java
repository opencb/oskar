package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.metadata.Individual;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.Oskar;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.opencb.oskar.spark.variant.analysis.ModeOfInheritanceTransformer.BIALLELIC;
import static org.opencb.oskar.spark.variant.analysis.ModeOfInheritanceTransformer.MONOALLELIC;

/**
 * Created on 21/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ModeOfInheritanceTransformerTest {

    private String familyId = "Family1";
    private String phenotype = "P1";
    private Dataset<Row> df;
    private Individual father;
    private Individual mother;
    private Individual child;

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Before
    public void setUp() throws Exception {
        Oskar oskar = sparkTest.getOskar();

        df = sparkTest.getVariantsDataset();
        VariantMetadata vm = oskar.metadata().variantMetadata(df);
        father = vm.getStudies().get(0).getIndividuals().get(0);
        mother = vm.getStudies().get(0).getIndividuals().get(1);
        child = vm.getStudies().get(0).getIndividuals().get(2);

        // Configure family in VariantMetadata
        father.setFamily(familyId);
        father.setFather(null);
        father.setMother(null);
        father.setPhenotype(phenotype);
        mother.setFamily(familyId);
        mother.setFather(null);
        mother.setMother(null);
        mother.setPhenotype(null);
        child.setFamily(familyId);
        child.setPhenotype(phenotype);
        df = oskar.metadata().setVariantMetadata(df, vm);
    }

    @Test
    public void biallelic() {
        df = new ModeOfInheritanceTransformer(BIALLELIC, familyId, phenotype).setMissingAsReference(true).transform(df);
//        df.explain(true);
        sparkTest.toVcf(df, father.getId(), mother.getId(), child.getId()).show(3);
    }

    @Test
    public void monoallelic() {
        df = new ModeOfInheritanceTransformer(MONOALLELIC, familyId, phenotype).setMissingAsReference(true).transform(df);
//        df.explain(true);
        sparkTest.toVcf(df, father.getId(), mother.getId(), child.getId()).show(3);
    }

}