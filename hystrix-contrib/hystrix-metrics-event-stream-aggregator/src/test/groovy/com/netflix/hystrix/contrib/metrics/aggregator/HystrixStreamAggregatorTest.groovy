package com.netflix.hystrix.contrib.metrics.aggregator;

import static org.junit.Assert.*

import org.junit.Test

import rx.Observable;
import rx.schedulers.Schedulers

class HystrixStreamAggregatorTest {

    @Test
    public void demo() {
        
        Observable<Map<String, Object>> hystrixStreamA = HystrixStreamSource.getHystrixStream(12345).subscribeOn(Schedulers.newThread());
        Observable<Map<String, Object>> hystrixStreamB = HystrixStreamSource.getHystrixStream(67890).skip(400).subscribeOn(Schedulers.newThread());

        Observable<Map<String, Object>> fullStream = Observable.merge(hystrixStreamA, hystrixStreamB);
        
        new HystrixStreamAggregator().run(fullStream);
    }
}
