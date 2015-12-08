package org.apache.eagle.datastream;

import junit.framework.Assert;
import org.apache.eagle.datastream.utils.Reflections;
import org.junit.Test;
import scala.reflect.api.TypeTags;

/**
 * @since 12/8/15
 */
public class TestJavaReflectionUtils {
    @Test
    public void testJavaFlatMapper(){
        Class<String> clazz = Reflections.javaTypeClass(new JavaEchoExecutor(), 0);
        Assert.assertEquals(String.class,clazz);
        TypeTags.TypeTag typeTag = Reflections.typeTag(clazz);
        Assert.assertNotNull(typeTag);
    }
}