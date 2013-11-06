
environments {

    spout.parallelism = 1
    bolt.parallelism = 1

    parallelism.EnrichLinkBoltLogic = 2

    maxPendingMessages = -1

    test {
        env.name = "test"
        url = "http://developer.usa.gov/1usagov"
    }

    development {
        env.name = "development"
        url = "http://developer.usa.gov/1usagov"
    }

    staging {
        env.name = "staging"
        url = "http://developer.usa.gov/1usagov"
    }

    production {
        env.name = "production"
        url = "http://developer.usa.gov/1usagov"
   }
}