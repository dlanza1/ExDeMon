package ch.cern.exdemon.monitor.trigger;

import java.time.Instant;

import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@ClassNameAlias("trigger-status")
@EqualsAndHashCode(callSuper=false)
public class TriggerStatus extends StatusValue {

    private static final long serialVersionUID = -753687009841163354L;
    
    @Getter @Setter
    private Instant lastRaised;
    
    @Getter @Setter
    private StatusValue activeStatus;
    
    @Getter @Setter
    private StatusValue sailentStatus;
    
    public TriggerStatus() {
        lastRaised = Instant.EPOCH;
    }

}
