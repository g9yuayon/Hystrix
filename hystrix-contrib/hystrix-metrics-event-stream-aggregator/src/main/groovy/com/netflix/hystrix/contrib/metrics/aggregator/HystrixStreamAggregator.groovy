package com.netflix.hystrix.contrib.metrics.aggregator

import java.util.concurrent.TimeUnit

import rx.Observable
import rx.functions.Func1
import rx.observables.GroupedObservable

class HystrixStreamAggregator {

    public static void main(String[] args) {
    }

    public void run(Observable<Map<String, Object>> stream) {

        System.out.println("Run HystrixStreaming Job!");

        // "type":"HystrixCommand","name":"MapGetPlaylist"
        Observable<Map<String, Object>> output = stream.filter({ Map<String, Object> data ->
            return data.get("type").equals("HystrixCommand");
        }).groupBy({ Map<String, Object> json ->
            return String.valueOf(json.get("name"));
        }).flatMap({ GroupedObservable<String, Map<String, Object>> group ->

            return group.groupBy({ Map<String, Object> json ->
                return String.valueOf(json.get("instanceId"));
            }).flatMap({ GroupedObservable<String, Map<String, Object>> instanceGroup ->
                /*
                 * For each instance we calculate the delta for each field.
                 */
                return instanceGroup.startWith(Collections.<String, Object> emptyMap()).buffer(2, 1)
                .map(new Func1<List<Map<String, Object>>, Map<String, Object>>() {

                    private Map<String, Object> delta = null;

                    @Override
                    public Map<String, Object> call(List<Map<String, Object>> data) {
                        Map<String, Object> previous = data.get(0);
                        if (data.size() < 2) {
                            System.out.println("NO DATA");
                            return Collections.emptyMap();
                        }
                        Map<String, Object> current = data.get(1);

                        if (delta == null) {
                            delta = new HashMap<String, Object>();
                            delta.putAll(previous);
                            delta.put("instanceId", group.getKey());
                            delta.put("name", current.get("name"));
                        }

                        for (String key : current.keySet()) {
                            if (key.equals("instanceId")) {
                                continue;
                            }
                            Object previousValue = previous.get(key);
                            Object currentValue = current.get(key);
                            if (currentValue instanceof Number) {
                                if (previousValue == null) {
                                    previousValue = 0;
                                }
                                Number previousValueAsNumber = (Number) previousValue;
                                if (currentValue != null) {
                                    Number currentValueAsNumber = (Number) currentValue;
                                    delta.put(key, (currentValueAsNumber.longValue() - previousValueAsNumber.longValue()));
                                }
                            } else {
                                // do string ... ignore for now
                            }
                        }

                        return delta;
                    }

                });

            }).scan(new HashMap<String, Object>(), {  Map<String, Object> state, Map<String, Object> delta ->
                for (String key : delta.keySet()) {
                    Object existing = state.get(key);
                    if (existing == null) {
                        existing = 0;
                    }
                    if (delta.get(key) instanceof Number) {
                        Number v = (Number) existing;
                        Number d = (Number) delta.get(key);
                        state.put(key, v.longValue() + d.longValue());
                    } else {
                        state.put(key, delta.get(key));
                    }
                }
                return state;

            }).throttleLast(500, TimeUnit.MILLISECONDS);

        });

        output.filter({ Map<String, Object> data ->
            return data.get("name") != null && data.get("name").equals("Search");
        }).toBlockingObservable().forEach({ Map<String, Object> t1 ->
            if (t1.keySet().size() > 0) {
                System.out.println("     DELTA [" + t1.get("instanceId") + "] =>    Success => " + t1.get("rollingCountSuccess") + "  Failure => " + t1.get("rollingCountFailure"));
            }
        });


    }
}
