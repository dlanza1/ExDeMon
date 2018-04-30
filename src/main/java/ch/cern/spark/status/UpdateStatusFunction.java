package ch.cern.spark.status;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.spark.status.StatusOperation.Op;

public abstract class UpdateStatusFunction<K extends StatusKey, V, S extends StatusValue, R>
    implements Function4<Time, K, Optional<StatusOperation<K, V>>, State<S>, Optional<RemoveAndValue<K, R>>> {

    private static final long serialVersionUID = 8556057397769787107L;
    
    @Override
    public Optional<RemoveAndValue<K, R>> call(Time time, K key, Optional<StatusOperation<K, V>> actionOrValue, State<S> state) throws Exception {
        if(state.isTimingOut()) {
            Optional<R> result = timingOut(time, key, state.get());
            
            return Optional.of(new RemoveAndValue<>(key, result));
        }
        
        if(actionOrValue.get().getOp().equals(Op.REMOVE)) {
            state.remove();
            
            return Optional.absent();
        }
        
        StatusImpl<S> status = new StatusImpl<S>(state, time);
        
        Optional<R> result = toOptional(update(key, actionOrValue.get().getValue(), status));
        
        if(status.isRemoved())
            return Optional.of(new RemoveAndValue<>(key, result));
        else
            return Optional.of(new RemoveAndValue<>(null, result));
    }

    protected abstract java.util.Optional<R> update(K key, V value, State<S> status) throws Exception;
    
    protected Optional<R> timingOut(Time time, K key, S state) {
        return Optional.empty();
    }
    
    private Optional<R> toOptional(java.util.Optional<R> result) {
        return result.isPresent() ? Optional.of(result.get()) : Optional.empty();
    }

}
