package com.netflix.hystrix.contrib.metrics.aggregator;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;

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

}
