package com.netflix.hystrix.contrib.metrics.aggregator

import com.netflix.discovery.EurekaClientConfig

/**
 * This class allows programmatically changing <a href="https://github.com/Netflix/eureka">Eureka</a> configurations.
 * Eureka supports only configuration files. This class allows arbitrary override of {@link EurekaClientConfig}'s
 * public method. The following example constructs a {@code GroovyEurekaClientConfig} that overrides the method
 * {@link com.netflix.discovery.DefaultEurekaClientConfig#getEurekaServerServiceUrls} and {@link com.netflix.discovery.DefaultEurekaClientConfig#getEurekaServerPort()}
 * <pre>
 * new GroovyEurekaClientConfig(
 *     new MyDataCenterInstanceConfig(),
 *     new GroovyEurekaClientConfig(
 *         delegate: new DefaultEurekaClientConfig(),
               overrides: [
                   "getEurekaServerServiceUrls": {region -> [eurekaUrl]},
                   "getEurekaServerPort": {port}
               ]) as EurekaClientConfig)
 * </pre>
 *
 */
class GroovyEurekaClientConfig {
    EurekaClientConfig delegate
    /**
     * A dictionary of method name and closure. Each closure should return
     * a value for the associated method name
     */
    def overrides

    def methodMissing(String name, def args) {
        if(name in overrides) {
            overrides[name](args)
        }else {
            delegate.invokeMethod(name, args)
        }
    }
}
