package me.affetti.flink.training.cep;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.codehaus.janino.util.Producer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by affo on 23/06/17.
 */
public class ShapesSource implements SourceFunction<Shape> {
    private boolean stop = false;
    private int count = 0;
    private Random rnd = new Random(0);
    private List<ShapeProducer<Shape>> producers;

    private final int limit;

    public ShapesSource(int limit) {
        this.limit = limit;

        producers = new ArrayList<>(3);
        producers.add(
                () -> new Square(randomColor(), randomSegmentMeasure())
        );
        producers.add(
                () -> new Circle(randomColor(), randomSegmentMeasure())
        );
        producers.add(
                () -> new Triangle(randomColor(), randomSegmentMeasure(), randomSegmentMeasure())
        );
    }

    private Color randomColor() {
        return Color.values()[rnd.nextInt(Color.values().length)];
    }

    private double randomSegmentMeasure() {
        return rnd.nextInt(10) + 1;
    }

    @Override
    public void run(SourceContext<Shape> sourceContext) throws Exception {
        while (!stop && count < limit) {
            Shape shape = getNextShape();
            sourceContext.collect(shape);
            count++;
        }
    }

    private Shape getNextShape() {
        int i = rnd.nextInt(producers.size());

        return producers.get(i).produce();
    }

    @Override
    public void cancel() {
        stop = true;
    }

    private interface ShapeProducer<S extends Shape> extends Producer<S>, Serializable {
    }
}
