package com.netflix.hystrix.contrib.metrics.aggregator

import io.netty.handler.codec.http.HttpMethod
import io.reactivex.netty.RxNetty
import io.reactivex.netty.pipeline.PipelineConfigurators
import io.reactivex.netty.protocol.http.client.HttpRequest
import io.reactivex.netty.protocol.http.client.HttpResponse
import io.reactivex.netty.protocol.text.sse.ServerSentEvent
import rx.functions.Action1
import rx.functions.Func1;

import static org.junit.Assert.*

import org.junit.Test

import rx.Observable;
import rx.schedulers.Schedulers

class HystrixStreamAggregatorTest {

    public static void main(String[] args) {
        new HystrixStreamAggregatorTest().demo();
    }

    @Test
    public void demo() {

        Observable<Map<String, Object>> hystrixStreamA = HystrixStreamSource.getHystrixStream(12345).subscribeOn(Schedulers.newThread());
        Observable<Map<String, Object>> hystrixStreamB = HystrixStreamSource.getHystrixStream(67890).skip(400).subscribeOn(Schedulers.newThread());

        Observable<Map<String, Object>> fullStream = Observable.parallelMerge(hystrixStreamA, hystrixStreamB);

        new HystrixStreamAggregator().aggregate(fullStream).filter({ Map<String, Object> data ->
            return data.get("rollingCountFailure") != 0;
            //            return data.get("name") != null && data.get("name").equals("Search");
        }).toBlockingObservable().forEach({ Map<String, Object> t1 ->
            if (t1.keySet().size() > 0) {
                System.out.println("     Sum [" + t1.get("name") + "] =>    Success => " + t1.get("rollingCountSuccess") + "  Failure => " + t1.get("rollingCountFailure"));
            }
        });
    }

    @Test
    void demoEureka() {
        String eurekaHost = "http://discovery.cloud.netflix.net:7001/discovery/v2/"
        int eurekaPort = 7001
        String vipAddress = 'apiproxy-stable.netflix.net:7001'
        Observable<Observable<Map<String, Object>>> streams = HystrixStreamSource.getHystrixStreams(eurekaHost, eurekaPort, vipAddress, "/eventbus.stream?topic=hystrix-metrics");

        new HystrixStreamAggregator().aggregate(streams).filter({ Map<String, Object> data ->
            return data.get("rollingCountFailure") != 0;
        }).toBlockingObservable().forEach({ Map<String, Object> t1 ->
            if (t1.keySet().size() > 0) {
                System.out.println("     Sum [" + t1.get("name") + "] =>    Success => " + t1.get("rollingCountSuccess") + "  Failure => " + t1.get("rollingCountFailure"));
            }
        });
    }
}
