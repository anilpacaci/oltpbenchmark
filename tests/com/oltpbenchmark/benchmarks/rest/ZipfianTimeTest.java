package com.oltpbenchmark.benchmarks.rest;

import org.junit.Test;

public class ZipfianTimeTest {
    
    @Test
    public void zipfianOutput() {
        
        for(int i = 0 ; i < 100 ; i++) {
            int random = RESTUtil.zipfianRandom(100, 3);
            System.out.println(random);
        }
    }
    
    public void zipfianPerformance() {
        for(int i = 0; i < 100 ; i++) {
            long start = System.nanoTime();
            RESTUtil.zipfianRandom(100, 3);
            long end = System.nanoTime();
            System.out.println(end - start);
        }
    }

}
