package org.apache.eagle.security.userprofile.model;

import org.apache.commons.math3.linear.RealMatrix;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

/**
 * @since 10/21/15
 */
public abstract class JavaUserProfileModeler<M,C extends UserProfileContext> implements UserProfileModeler<M,C> {
    @Override
    public List<M> build(String site, String user, RealMatrix matrix) {
        java.util.List<M> models = generate(site, user, matrix);
        if (models != null) {
            return JavaConversions.asScalaIterable(models).toList();
        } else {
            return null;
        }
    }

    /**
     * Java delegate method for {@code eagle.security.userprofile.model.JavaUserProfileModeler#build(String site, String user, RealMatrix matrix)}
     *
     * @param site eagle site
     * @param user eagle user
     * @param matrix user activity matrix
     * @return Generate user profile model
     */
    public abstract java.util.List<M> generate(String site, String user, RealMatrix matrix);
}