package com.clearstorydata.dataflow.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ianlcsd on 7/17/15.
 */
public class GcsFileUtilTest {
    //@ClassRule
    //public static WireMockClassRule wireMockRule = new WireMockClassRule(PORT);

    @Test
    public void arrangeBatch() throws Exception {
        List<String> srcFileNames = Arrays.asList("1", "2", "3", "4", "5", "6", "7");
        String destFilename = "dest";
        List<List<String>> arrangedBatch;

        int batchSize = 5;
        arrangedBatch = GcsFileUtil.arrangeBatch(
                batchSize, srcFileNames, destFilename);
        System.out.println(Arrays.toString(arrangedBatch.toArray()));
        String[][] expected = new String[][]{
                {"dest", "1", "2", "3", "4"},
                {"dest", "5", "6", "7"}
        };
        for (int i = 0; i < arrangedBatch.size(); i++) {
            Assert.assertArrayEquals(
                    expected[i],
                    arrangedBatch.get(i).toArray(new String[0]));
        }


        batchSize = 2;
        arrangedBatch = GcsFileUtil.arrangeBatch(
                batchSize, srcFileNames, destFilename);
        System.out.println(Arrays.toString(arrangedBatch.toArray()));
        expected = new String[][]{
                {"dest", "1"},
                {"dest", "2"},
                {"dest", "3"},
                {"dest", "4"},
                {"dest", "5"},
                {"dest", "6"},
                {"dest", "7"}
        };
        for (int i = 0; i < arrangedBatch.size(); i++) {
            Assert.assertArrayEquals(
                    expected[i],
                    arrangedBatch.get(i).toArray(new String[0]));
        }


        batchSize = 7;
        arrangedBatch = GcsFileUtil.arrangeBatch(
                batchSize, srcFileNames, destFilename);
        System.out.println(Arrays.toString(arrangedBatch.toArray()));
        expected = new String[][]{
                {"dest", "1", "2", "3", "4", "5", "6"},
                {"dest", "7"}
        };
        for (int i = 0; i < arrangedBatch.size(); i++) {
            Assert.assertArrayEquals(
                    expected[i],
                    arrangedBatch.get(i).toArray(new String[0]));
        }

        batchSize = 8;
        arrangedBatch = GcsFileUtil.arrangeBatch(
                batchSize, srcFileNames, destFilename);
        System.out.println(Arrays.toString(arrangedBatch.toArray()));
        expected = new String[][]{
                {"dest", "1", "2", "3", "4", "5", "6", "7"}
        };
        for (int i = 0; i < arrangedBatch.size(); i++) {
            Assert.assertArrayEquals(
                    expected[i],
                    arrangedBatch.get(i).toArray(new String[0]));
        }

        srcFileNames = Arrays.asList();
        batchSize = 2;
        arrangedBatch = GcsFileUtil.arrangeBatch(
                batchSize, srcFileNames, destFilename);
        System.out.println(Arrays.toString(arrangedBatch.toArray()));
        expected = new String[][]{};
        for (int i = 0; i < arrangedBatch.size(); i++) {
            Assert.assertArrayEquals(
                    expected[i],
                    arrangedBatch.get(i).toArray(new String[0]));
        }

    }

}
