package ch.cern.exdemon.metrics.predictor;

public class Prediction {

    private float value;
    private float standardDeviation;

    public Prediction(float value, float standardDeviation) {
        this.value = value;
        this.standardDeviation = standardDeviation;
    }

    public float getValue() {
        return value;
    }

    public float getStandardDeviation() {
        return standardDeviation;
    }

}
