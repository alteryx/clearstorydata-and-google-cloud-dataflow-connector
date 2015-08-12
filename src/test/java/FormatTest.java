/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

import com.clearstorydata.dataflow.io.Format;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.clearstorydata.dataflow.io.Format.ofCsv;

/**
 * Created by ianlcsd on 6/15/15.
 */
public class FormatTest {

    @Test
    public void testCsv() {
        Format<List<String>> csvFormat = ofCsv();

        Assert.assertNotNull(csvFormat);

        List<String> input = Arrays.asList("1", "2", "3", "4", "5", "6");
        String encoded = csvFormat.getEncoder().encode(input);
        Assert.assertEquals("1,2,3,4,5,6\n", encoded);
        System.out.println(encoded);

        Format.RowReader<List<String>> reader = csvFormat.createReader(
                new ByteArrayInputStream(
                        encoded.getBytes(StandardCharsets.UTF_8)));
        List<List<String>> output = new ArrayList<>();
        while (reader.hasNext()) {
            output.add(reader.next());
        }

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(input, output.get(0));
    }
}
