package com.spark.tutorial.ubs;

import java.math.BigDecimal;

public class Test {
    public static void main(String[] args) {
      for(int i=2;i<10;i++){
          System.out.println(random(i));
      }

    }
    public static BigDecimal random(int range) {
        BigDecimal max = new BigDecimal(range);
        BigDecimal randFromDouble = new BigDecimal(Math.random());
        BigDecimal actualRandomDec = randFromDouble.multiply(max);
        actualRandomDec = actualRandomDec
                .setScale(2, BigDecimal.ROUND_DOWN);
        return actualRandomDec;
    }
}
