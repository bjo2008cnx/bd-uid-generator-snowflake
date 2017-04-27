package com.baidu.fsg.uid;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * Test for {@link com.baidu.fsg.uid.impl.DefaultUidGenerator}
 *
 * @author yutianbao
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:uid/cached-uid-spring.xml"})
public class CachedSimpleUidGeneratorTest {

    @Resource
    private UidGenerator uidGenerator;

    /**
     * Do generating
     */
    @Test
    public void doGenerate() {
        long uid = uidGenerator.getUID();
        System.out.println(uid);
        String parsedInfo = uidGenerator.parseUID(uid);

        // Check UID is positive, and can be parsed
        Assert.assertTrue(uid > 0L);
        Assert.assertTrue(StringUtils.isNotBlank(parsedInfo));

        System.out.println(Thread.currentThread().getName() + " No.  >>> " + parsedInfo);
    }
}
