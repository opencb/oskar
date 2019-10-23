package org.opencb.oskar.spark.variant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.core.exceptions.OskarException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.opencb.oskar.spark.OskarSparkTestUtils.PLATINUM_SMALL;

/**
 * Created on 13/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataManagerTest {

    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    private Dataset<Row> df;
    private VariantMetadataManager vmm;


    @Before
    public void setUp() throws Exception {
        df = sparkTest.getVariantsDataset();
        vmm = new VariantMetadataManager();
    }

    @Test
    public void readVariantMetadata() throws OskarException, IOException {
        VariantMetadata expected = vmm.readMetadata(vmm.getMetadataPath(sparkTest.getFile(PLATINUM_SMALL).getAbsolutePath()));
        VariantMetadata actual = vmm.variantMetadata(df);

        assertEquals(expected, actual);
    }

    @Test
    public void getSamples() throws OskarException, IOException {
        List<String> expected = IntStream.range(12877, 12894).mapToObj(i -> "NA" + i).collect(Collectors.toList());
        Map<String, List<String>> samples = vmm.samples(df);

        assertEquals(1, samples.size());
        assertEquals(expected, samples.values().iterator().next());
    }

}
