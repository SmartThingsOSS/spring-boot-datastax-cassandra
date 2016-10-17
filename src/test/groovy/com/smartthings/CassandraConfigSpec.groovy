package com.smartthings

import com.datastax.driver.core.Session

import spock.lang.Specification

class CassandraConfigSpec extends Specification {

	def 'should get session'() {
		setup:
		CassandraConfig config = Spy()
		Session expectedSession = Mock()
		config.endpoints = ['127.0.0.1']
		config.keyspace = 'test'
		config.autoMigrate = false

		when:
		Session session = config.session()

		then:
		1 * config.connect(_) >> expectedSession
		session != null
	}
}
