package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.api.java.UDF1;
import scala.runtime.AbstractFunction1;

/**
 * Created on 06/09/18.
 *
 * Reverse and complementary
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RevcompFunction
        extends AbstractFunction1<String, String>
        implements UDF1<String, String> {

    @Override
    public String call(String allele) {
        if (allele.length() == 1) {
            switch (allele) {
                case "C":
                case "c":
                    return "G";
                case "G":
                case "g":
                    return "C";
                case "T":
                case "t":
                    return "A";
                case "A":
                case "a":
                    return "T";
                default:
                    return allele;
            }
        } else {
            StringBuilder sb = new StringBuilder(allele.length());

            // reverse the allele
            for (int i = allele.length() - 1; i >= 0; i--) {
                char c = allele.charAt(i);
                switch (c) {
                    case 'C':
                    case 'c':
                        sb.append('G');
                        break;
                    case 'G':
                    case 'g':
                        sb.append('C');
                        break;
                    case 'T':
                    case 't':
                        sb.append('A');
                        break;
                    case 'A':
                    case 'a':
                        sb.append('T');
                        break;
                    default:
                        sb.append(c);
                        break;
                }
            }

            return sb.toString();
        }
    }

    @Override
    public String apply(String allele) {
        return call(allele);
    }
}
