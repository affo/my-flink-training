package me.affetti.flink.training.cep;

/**
 * Created by affo on 21/10/17.
 */
public class Triangle extends Shape {
    public final double base, height;

    public Triangle(Color c, double base, double height) {
        super(c);
        this.base = base;
        this.height = height;
    }

    @Override
    public double getSurface() {
        return base * height * 0.5;
    }
}
