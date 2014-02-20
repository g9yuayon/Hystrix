package com.netflix.hystrix.contrib.metrics.aggregator

import java.util.concurrent.TimeUnit

import rx.Observable
import rx.functions.Func1
import rx.observables.GroupedObservable

class HystrixStreamAggregator {

    public static void main(String[] args) {
    }

    /**
     * 
     * @param stream The inner Observable<Map<String, Object>> represents a single instance (network connection)
     * @return
     */
    public Observable<Map<String, Object>> aggregate(Observable<Observable<Map<String, Object>>> stream) {

        System.out.println("Run HystrixStreaming Job!");

        return stream.filter({ Map<String, Object> data ->
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
                return instanceGroup.startWith(Collections.<String, Object> emptyMap()).buffer(2, 1).map(previousAndCurrentToDelta());
            }).scan(new HashMap<String, Object>(), {  Map<String, Object> state, Map<String, Object> delta ->
                for (String key : delta.keySet()) {
                    Object existing = state.get(key);
                    if (existing == null) {
                        existing = 0;
                    }
                    if(key.equals("instanceId")) {
                        continue;
                    }
                    if (delta.get(key) instanceof Number) {
                        Number v = (Number) existing;
                        Number d = (Number) delta.get(key);
                        state.put(key, v.longValue() + d.longValue());
                    } else {
                        // TODO handle strings
                        state.put(key, delta.get(key));
                    }
                }
                return state;
            }).throttleLast(500, TimeUnit.MILLISECONDS);
        });

    }


    private final Func1<List<Map<String, Object>>, Map<String, Object>> previousAndCurrentToDelta() {
        Map<String, Object> delta = null;
        return { List<Map<String, Object>> data ->
            Map<String, Object> previous = data.get(0);
            if (data.size() < 2) {
                System.out.println("NO DATA");
                return Collections.emptyMap();
            }
            Map<String, Object> current = data.get(1);

            if (delta == null) {
                delta = new HashMap<String, Object>();
                delta.putAll(previous);
                delta.put("instanceId", current.get("instanceId"));
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
    }
}
