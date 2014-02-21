package com.netflix.hystrix.contrib.metrics.aggregator

import com.netflix.appinfo.InstanceInfo
import org.junit.Test
import static com.netflix.hystrix.contrib.metrics.aggregator.NetflixEurekaSource.*

class NetflixEurekaSourceTest {

    @Test
    void testDiscoveryClientAndFilter() {
        NetflixEurekaSource source = new NetflixEurekaSource("http://discovery.cloud.netflix.net:7001/discovery/v2/", 7001)

        def apps1 = source.instancesByAppName("API", "MerchWeb").collect{instance ->
            instance.getAppName()
        } as Set

        def apps2 = source.instances{ InstanceInfo instance ->
            instance.getAppName() == "API" || instance.getAppName() == "MERCHWEB"
        }.collect{ instance ->
            instance.getAppName()
        } as Set

        assert(apps1 == apps2)

        def apps = source.instances(none(
                {InstanceInfo instance ->
                    instance.getAppName() == "CRYPTEX"
                },
                {InstanceInfo instance ->
                    instance.getAppName() == "NCCP"
                }
        )).collect{instance ->
            instance.getAppName()
        } as Set

        assert(!("CRYPTEX" in apps && "NCCP" in apps))


        assert(([InstanceInfo.InstanceStatus.UP] as Set) == (source.instances(statusFilter(InstanceInfo.InstanceStatus.UP)).collect{ instance ->
            instance.getStatus()
        } as Set))
    }
}
