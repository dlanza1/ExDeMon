package ch.cern.spark.metrics.notificator;

import java.time.Instant;

import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@ClassNameAlias("notificator-status")
@EqualsAndHashCode(callSuper=false)
public class NotificatorStatus extends StatusValue {

    private static final long serialVersionUID = -753687009841163354L;
    
    @Getter @Setter
    private Instant lastRaised;
    
    @Getter @Setter
    private StatusValue activeStatus;
    
    @Getter @Setter
    private StatusValue sailentStatus;
    
    public NotificatorStatus() {
        lastRaised = Instant.EPOCH;
    }

}
