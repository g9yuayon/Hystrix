package com.netflix.hystrix.contrib.metrics.aggregator;


import com.netflix.appinfo.InstanceInfo;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.http.client.HttpRequest;
import io.reactivex.netty.protocol.http.client.HttpResponse;
import io.reactivex.netty.protocol.text.sse.ServerSentEvent;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class HystrixStreamSource {
    public static void main(String[] args) {
        getHystrixStream(12345).take(5).toBlockingObservable().forEach(new Action1<Map<String, Object>>() {

            @Override
            public void call(Map<String, Object> s) {
                System.out.println("s: " + s.keySet());
            }

        });
    }

    public static Observable<Map<String, Object>> getHystrixStream(final int instanceID) {
        return Observable.create(new OnSubscribe<Map<String, Object>>() {

            @Override
            public void call(Subscriber<? super Map<String, Object>> sub) {
                try {
                    while (!sub.isUnsubscribed()) {
                        InputStream file = HystrixStreamSource.class.getResourceAsStream("/com/netflix/hystrix/contrib/metrics/aggregator/hystrix.stream");
                        BufferedReader in = new BufferedReader(new InputStreamReader(file));
                        String line = null;
                        while ((line = in.readLine()) != null && !sub.isUnsubscribed()) {
                            if (!line.trim().equals("")) {
                                if (line.startsWith("data: ")) {
                                    String json = line.substring(6);
                                    try {
                                        Map<String, Object> jsonMap = JSONUtility.mapFromJson(json);
                                        jsonMap.put("instanceId", instanceID);
                                        sub.onNext(jsonMap);
                                    } catch (Exception e) {
//                                        System.err.println("bad data");
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    sub.onError(e);
                }
            }

        });
    }

    public static Observable<Map<String, Object>> getHystrixStream(final InstanceInfo instance, String hystrixUrl) {
        HttpRequest<Object> request = HttpRequest.create(HttpMethod.GET, hystrixUrl);
        request.getHeaders().add(HttpHeaders.Names.HOST, instance.getHostName());
        return RxNetty.createHttpClient(instance.getHostName(), instance.getPort(), PipelineConfigurators.sseClientConfigurator())
            .submit(request)
            .flatMap(new Func1<HttpResponse<ServerSentEvent>, Observable<ServerSentEvent>>() {
                @Override
                public Observable<ServerSentEvent> call(HttpResponse<ServerSentEvent> response) {
                    // TODO handle error response
                    System.out.println("Response: "+response.getStatus());
                    return response.getContent();
                }
            })
            .map(new Func1<ServerSentEvent, Map<String, Object>>() {
                @Override
                public Map<String, Object> call(ServerSentEvent e) {
                    Map<String, Object> jsonMap = JSONUtility.mapFromJson(e.getEventData());
                    jsonMap.put("instanceId", instance.getId());

                    return jsonMap;
                }
            });

    }

    public static Observable<Observable<Map<String, Object>>> getHystrixStreams(String eurekaHost, int eurekaPort, String vipAddress, final String hystrixUrl) {
        NetflixEurekaSource discovery = new NetflixEurekaSource(eurekaHost, eurekaPort);

        return Observable.from(discovery.instancesByVipAddress(vipAddress))
            .filter(new Func1<InstanceInfo, Boolean>() {
                @Override
                public Boolean call(InstanceInfo instanceInfo) {
                    return instanceInfo.getStatus() == InstanceInfo.InstanceStatus.UP;
                }
            })
            .map(new Func1<InstanceInfo, Observable<Map<String, Object>>>() {
                @Override
                public Observable<Map<String, Object>> call(InstanceInfo instanceInfo) {
                    return getHystrixStream(instanceInfo, hystrixUrl);
                }
            });
    }
}
