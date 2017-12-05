package ict.ada.gdb.hbase;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lon on 17-4-24.
 */
public class TableMapperSplit extends TableInputFormat {


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> inputSplits = super.getSplits(context);
        List<InputSplit> newInputSplits = new ArrayList<InputSplit>();
        super.initialize(context);
        TableName tName = super.getTable().getName();

        int mergeStep = context.getConfiguration().getInt(
                "hbase.mapreduce.icare.mergestep", 5);

        long totalRegionSize = 0;
        for (int i = 0; i < inputSplits.size(); i++) {
            TableSplit ts = (TableSplit) inputSplits.get(i);
            totalRegionSize += ts.getLength();
        }
        long averageRegionSize = totalRegionSize / inputSplits.size();
        long spiltTotalSize = averageRegionSize * mergeStep;

        int index = 0;
        while (index < inputSplits.size()) {
            TableSplit ts = (TableSplit) inputSplits.get(index);
            long totalSize = ts.getLength();
            byte[] splitStartKey = ts.getStartRow();
            byte[] splitEndKey = ts.getEndRow();
            index++;
            for (; index < inputSplits.size(); index++) {
                TableSplit nextRegion = (TableSplit) inputSplits.get(index);
                long nextRegionSize = nextRegion.getLength();
                if (totalSize + nextRegionSize < spiltTotalSize) {
                    totalSize = totalSize + nextRegionSize;
                    splitEndKey = nextRegion.getEndRow();
                } else {
                    break;
                }
            }

            TableSplit tsNew = new TableSplit(tName, splitStartKey,
                    splitEndKey, ts.getRegionLocation(), totalSize);

            newInputSplits.add(tsNew);
        }

        super.closeTable();
        return newInputSplits;
    }

}
