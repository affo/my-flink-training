package me.affetti.flink.training.cep;

/**
 * Created by affo on 23/06/17.
 */
public abstract class Shape {
    public final Color color;

    public Shape() {
        color = Color.BLUE;
    }

    public Shape(Color color) {
        this.color = color;
    }

    public abstract double getSurface();

    @Override
    public String toString() {
        return color.name() + " " + getClass().getSimpleName() + " (" + getSurface() + ")";
    }
}
