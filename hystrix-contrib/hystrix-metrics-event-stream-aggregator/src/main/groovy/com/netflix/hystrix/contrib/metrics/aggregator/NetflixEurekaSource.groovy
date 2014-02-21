package com.netflix.hystrix.contrib.metrics.aggregator

import com.netflix.appinfo.EurekaInstanceConfig
import com.netflix.appinfo.InstanceInfo
import com.netflix.appinfo.MyDataCenterInstanceConfig
import com.netflix.discovery.DefaultEurekaClientConfig
import com.netflix.discovery.DiscoveryClient
import com.netflix.discovery.DiscoveryManager
import com.netflix.discovery.EurekaClientConfig

/**
 * A wrapper of <a href="https://github.com/Netflix/eureka">Netflix Eureka</a> library.
 */
class NetflixEurekaSource {
    List<Closure<InstanceInfo>> filters
    final DiscoveryClient discovery

    NetflixEurekaSource(String eurekaUrl, int port) {
        this(
                new MyDataCenterInstanceConfig(),
                new GroovyEurekaClientConfig(
                        delegate: new DefaultEurekaClientConfig(),
                        overrides: [
                                "getEurekaServerServiceUrls": {region -> [eurekaUrl]},
                                "getEurekaServerPort": {port}
                        ]) as EurekaClientConfig)
    }

    NetflixEurekaSource(EurekaInstanceConfig instanceConfig, EurekaClientConfig clientConfig) {
        filters = []

        DiscoveryManager.getInstance().initComponent(instanceConfig, clientConfig)

        discovery = DiscoveryManager.getInstance().getDiscoveryClient()
    }

    List<InstanceInfo> instances() {
        discovery.getApplications().getRegisteredApplications().collect {application ->
            application.getInstances()
        }.flatten()
    }

    List<InstanceInfo> instancesByVipAddress(String vipAddress, boolean secure=false) {
        discovery.getInstancesByVipAddress(vipAddress, secure)
    }

    List<InstanceInfo> instancesByVipAddress(String vipAddress, Closure filter, boolean secure=false) {
        instancesByVipAddress(vipAddress, secure).findAll(filter)
    }

    List<InstanceInfo> instancesByAppName(String[] appNames, filter=null) {
        def instances = appNames.collect {app ->
            discovery.getApplication(app).getInstances()
        }.flatten()

        if(filter == null) {
            return instances
        }

        return instances.findAll(filter)
    }

    List<InstanceInfo> instances(Closure filter){
        instances().findAll(filter)
    }

    static Closure statusFilter(InstanceInfo.InstanceStatus status) {
        {InstanceInfo instance ->
            instance.getStatus().is(status)
        }
    }

    static Closure asgFilter(String asg) {
        {InstanceInfo instance ->
            instance.getASGName() == asg
        }
    }

    static Closure any(Closure[] filters){
        {InstanceInfo instance ->
            for(filter in filters) {
                if(filter(instance)) return true
            }

            return false
        }
    }

    static Closure all(Closure[] filters) {
        {InstanceInfo instance ->
            for(filter in filters) {
                if(!filter(instance)) return false
            }

            return true
        }
    }

    static Closure not(Closure filter) {
        {InstanceInfo instance ->
            !filter(instance)
        }
    }

    static Closure none(Closure[] filters) {
        {InstanceInfo instance ->
            not(any(filters))(instance)
        }
    }

    static Closure inAsgsFilter(String[] asgs) {
        def asgSet = asgs as Set
        {InstanceInfo instance ->
            return instance.getASGName() in asgs
        }
    }
}
