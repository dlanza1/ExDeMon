package ch.cern.exdemon.monitor.analysis.types.htm;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.numenta.nupic.model.Persistable;
import org.numenta.nupic.network.Persistence;
import org.numenta.nupic.network.PersistenceAPI;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PersistableKryoSerializer extends Serializer<Persistable> {
    
    private transient final static Logger LOG = Logger.getLogger(PersistableKryoSerializer.class.getName());

    private static PersistenceAPI persistence = Persistence.get();
    
    @Override
    public void write(Kryo kryo, Output output, Persistable persistable) {
        persistable.preSerialize();
        
        byte[] bytes = persistence.serializer().serialize(persistable);
        
        OutputStream os = output.getOutputStream();
        try {
            os.write(bytes);
        } catch (IOException e) {
            LOG.error("Serialization error", e);
        }
    }
    
    @Override
    public Persistable read(Kryo kryo, Input input, Class<Persistable> clazz) {
        byte[] bytes = input.getBuffer();
        
        Persistable persistable = persistence.read(bytes);
        
        return persistable;
    }

}
