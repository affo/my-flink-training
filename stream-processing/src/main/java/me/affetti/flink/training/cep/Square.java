package me.affetti.flink.training.cep;

/**
 * Created by affo on 21/10/17.
 */
public class Square extends Shape {
    public final double base;

    public Square(Color color, double base) {
        super(color);
        this.base = base;
    }


    @Override
    public double getSurface() {
        return base * base;
    }
}
