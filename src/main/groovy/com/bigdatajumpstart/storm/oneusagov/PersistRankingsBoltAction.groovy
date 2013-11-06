package com.bigdatajumpstart.storm.oneusagov

import backtype.storm.tuple.Tuple

import backtype.storm.task.OutputCollector
import com.bigdatajumpstart.storm.StreamingDataAction
import org.springframework.beans.factory.annotation.Autowired
import storm.starter.tools.Rankable

import java.sql.Timestamp
import javax.sql.DataSource

import groovy.sql.Sql

import storm.starter.tools.Rankings
/**
 * Implements the business logic of writing a tuple to the console.
 */
class PersistRankingsBoltAction implements StreamingDataAction {

    @Autowired
    DataSource dataSource

    Integer windowSizeSecs = 120;

    void execute(OutputCollector collector, Tuple input) {

        Rankings rankings = (Rankings)input.getValue(0)
        if (!rankings) return;

        Timestamp windowTimestamp = new Timestamp(System.currentTimeMillis())
        Sql sql = new Sql(dataSource)
        try {
            rankings.rankings.each { Rankable rank ->
                String globalBitlyHash = (String)rank.getObject()
                Integer freqInWindow = (new Long(rank.getCount())).intValue()

                sql.executeInsert("insert into page_count (page_id,window_timestamp,window_size_secs,freq) values (?,?,?,?)",
                        [globalBitlyHash, windowTimestamp, windowSizeSecs, freqInWindow])
            }
        } finally {
            sql.close()
        }
    }
}
