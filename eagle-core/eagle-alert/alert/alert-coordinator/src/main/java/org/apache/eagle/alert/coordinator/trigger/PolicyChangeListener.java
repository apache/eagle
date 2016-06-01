package org.apache.eagle.alert.coordinator.trigger;

import java.util.Collection;
import java.util.List;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

public interface PolicyChangeListener {
    void onPolicyChange(List<PolicyDefinition> allPolicies, Collection<String> addedPolicies, Collection<String> removedPolicies, Collection<String> modifiedPolicies);
}
