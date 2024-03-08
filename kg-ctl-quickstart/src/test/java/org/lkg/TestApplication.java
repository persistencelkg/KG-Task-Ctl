package org.lkg;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.lkg.job.SyncHolidayJob;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/2/27 5:30 PM
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestApplication {

    @Resource private SyncHolidayJob syncHolidayJob;
    @Test
    public void testSync() {
        syncHolidayJob.run();
    }
}
