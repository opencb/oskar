package org.opencb.oskar.spark.variant.analysis;

import org.apache.spark.sql.Row;
import org.opencb.commons.utils.CollectionUtils;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

public class SparkAnalysisUtils {

    public static void writeRows(Iterator<Row> rowIterator, PrintWriter pw) {
        StringBuilder line = new StringBuilder();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            line.setLength(0);
            for (int i = 0; i < row.size(); i++) {
                if (line.length() != 0) {
                    line.append("\t");
                }
                if (row.get(i) instanceof scala.collection.immutable.Map) {
                    scala.collection.Map<Object, Object> map = row.getMap(i);
                    if (map != null && map.size() != 0) {
                        scala.collection.Iterator<Object> iterator = map.keys().iterator();
                        while (iterator.hasNext()) {
                            Object key = iterator.next();
                            line.append(key).append(":").append(map.get(key).get());
                            if (iterator.hasNext()) {
                                line.append(";");
                            }
                        }
                    }
                } else if (row.get(i) instanceof scala.collection.mutable.WrappedArray) {
                    List<Object> list = row.getList(i);
                    if (CollectionUtils.isNotEmpty(list)) {
                        for (int j = 0; j < list.size(); j++) {
                            if (j > 0) {
                                line.append(";");
                            }
                            line.append(list.get(j));
                        }
                    }
                } else {
                    line.append(row.get(i));
                }
            }
            pw.println(line);
        }
    }
}
