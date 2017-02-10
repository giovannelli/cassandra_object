Bundler.require :cassandra

CassandraObject::Base.config = {
  keyspace: 'cassandra_object_test',
  hosts: ['127.0.0.1'],
  compression: :lz4,
  connect_timeout: 0.1,
  timeout: 1,
  consistency: :quorum,
  protocol_version: 3,
  page_size: 10000,
  trace: true,
  connections_per_local_node: 2,
  connections_per_remote_node: 2,
  logger: Logger.new($stderr)
}

begin
  CassandraObject::Schema.drop_keyspace 'cassandra_object_test'
rescue Exception => e
  puts e.message
end

sleep 1
CassandraObject::Schema.create_keyspace 'cassandra_object_test'
CassandraObject::Schemaless.create_column_family 'Issues'
CassandraObject::Schema.create_column_family 'IssueSchemas', {attributes: 'id text, title text, description text, field float, intero int, created_at timestamp, updated_at timestamp, PRIMARY KEY (id)', options: {}}
CassandraObject::Schemaless.create_column_family 'IssueDynamics'
CassandraObject::Schema.create_column_family 'IssueSchemaFathers', {attributes: 'id text, title text, field float, created_at timestamp, updated_at timestamp, PRIMARY KEY (id)', options: {}}
CassandraObject::Schema.create_column_family 'IssueSchemaChildren', {attributes: 'id text, title text, description text, field float, created_at timestamp, updated_at timestamp, issue_schema_father_id text, PRIMARY KEY (id)', options: {}}
CassandraObject::BaseSchemaless.adapter.consistency = :quorum
CassandraObject::BaseSchemalessDynamic.adapter.consistency = :quorum
CassandraObject::BaseSchema.adapter.consistency = :quorum

CassandraObject::Base.class_eval do
  class_attribute :created_records
  self.created_records = []

  after_create do
    created_records << self
  end

  def self.delete_after_test
    # created_records.reject(&:destroyed?).each(&:destroy)
    Issue.delete_all
    created_records.clear
  end
end

module ActiveSupport
  class TestCase
    teardown do
      if CassandraObject::Base.created_records.any?
        CassandraObject::Base.delete_after_test
      end
    end
  end
end
