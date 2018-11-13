package org.opencb.oskar.spark.variant;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.commons.utils.ListUtils;
import org.opencb.oskar.spark.OskarSparkTestUtils;
import org.opencb.oskar.spark.commons.OskarException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OskarTest {
    @ClassRule
    public static OskarSparkTestUtils sparkTest = new OskarSparkTestUtils();

    @Test
    public void testSamples() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        List<String> samples = sparkTest.getOskar().metadata().samples(df, "hgvauser@platinum:illumina_platinum");
        for (String sample: samples) {
            System.out.println(sample);
        }
    }

    @Test
    public void testPedigree() throws IOException, OskarException {
        Dataset<Row> df = sparkTest.getVariantsDataset();

        Map<String, List<Pedigree>> pedigreeMap = sparkTest.getOskar().metadata().pedigrees(df);
        for (String studyId : pedigreeMap.keySet()) {
            System.out.println("Study: " + studyId);
            for (Pedigree pedigree : pedigreeMap.get(studyId)) {
                System.out.println("\tFamily: " + pedigree.getName());
                System.out.println("\tMembers: ");
                for (Member member: pedigree.getMembers()) {
                    System.out.println("\t\t" + member.getId() + "\t" + member.getName()
                            + "\tfather: " + (member.getFather() == null ? "-" : member.getFather().getId())
                            + "\tmother:" + (member.getMother() == null ? "-" : member.getMother().getId())
                            + "\tsex:" + member.getSex());
                    if (member.getMultiples() != null && ListUtils.isNotEmpty(member.getMultiples().getSiblings())) {
                        System.out.println("\t\t\tsiblings: "
                                + StringUtils.join(member.getMultiples().getSiblings(), ","));
                    }
                    System.out.println();
                }
            }
        }
    }
}