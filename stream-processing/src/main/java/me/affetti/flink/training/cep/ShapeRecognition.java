package me.affetti.flink.training.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Created by affo on 23/06/17.
 */
public class ShapeRecognition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Shape> input = env.addSource(new ShapesSource(1000));

        Pattern<Shape, ?> twoBlueSquares = Pattern.<Shape>begin("start")
                .subtype(Square.class)
                .where(
                        new SimpleCondition<Square>() {
                            @Override
                            public boolean filter(Square square) throws Exception {
                                return square.color == Color.BLUE;
                            }
                        });
        //.times(2).consecutive();

        PatternStream<Shape> ps = CEP.pattern(input, twoBlueSquares);
        ps.select(new PatternSelectFunction<Shape, String>() {
            @Override
            public String select(Map<String, List<Shape>> map) throws Exception {
                return ">> " + map.get("start").toString() + " <<";
            }
        }).print();

        env.execute();
    }
}
