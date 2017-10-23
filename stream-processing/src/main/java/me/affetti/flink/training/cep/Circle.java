package me.affetti.flink.training.cep;

/**
 * Created by affo on 21/10/17.
 */
public class Circle extends Shape {
    public final double radius;

    public Circle(Color color, double radius) {
        super(color);
        this.radius = radius;
    }

    @Override
    public double getSurface() {
        return Math.PI * radius * radius;
    }
}
