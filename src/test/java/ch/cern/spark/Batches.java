package ch.cern.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Batches<T> implements Serializable{

    private static final long serialVersionUID = 7460188812026964754L;
    
    private List<List<T>> batches = new ArrayList<>();
    
    public void add(int batch, T element) {
        if(batches.size() <= batch)
            batches.add(batch, new LinkedList<>());
        
        batches.get(batch).add(element);
    }

    public int size() {
        return batches.size();
    }

    public List<T> get(int batch) {
        return batches.get(batch);
    }

    public void addBatch(List<T> batch) {
        batches.add(batches.size(), batch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        Batches other = (Batches) obj;
        if (batches == null) {
            if (other.batches != null)
                return false;
        } else if (!batches.equals(other.batches))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Batches [batches=" + batches + "]";
    }

}
