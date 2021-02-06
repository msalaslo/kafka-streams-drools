package com.github.msalaslo.streamedrules.drools;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

/**
 * Responsible for getting a Drools session and applying the Drools rules.
 */
public class DroolsRulesApplier {
    private static KieSession KIE_SESSION;

    public DroolsRulesApplier(String sessionName) {
		KieServices ks = KieServices.Factory.get();
		KieContainer kContainer = ks.getKieClasspathContainer();
		KIE_SESSION = kContainer.newKieSession(sessionName);
    }

    /**
     * Applies the loaded Drools rules to a given String.
     *
     * @param value the String to which the rules should be applied
     * @return the String after the rule has been applied
     */
    public String applyRule(String value) {
        Message message = new Message(value);
        KIE_SESSION.insert(message);
        KIE_SESSION.fireAllRules();
        return message.getContent();
    }
}
