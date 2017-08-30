class IssueCustomConfig < CassandraObject::BaseSchemalessDynamic
  string :description
  string :title

  before_create { self.description ||= 'funny' }

  self.allow_filtering = true

  def self.for_key key
    where_ids(key)
  end

  # must be different to general config to test funcionality!
  def self.custom_config
    {
        keyspace: 'cassandra_object_test',
        hosts: ['127.0.0.1'],
        compression: :lz4,
        connect_timeout: 2,
        timeout: 30,
        consistency: :quorum,
        protocol_version: 3,
        page_size: 12345,
        trace: true,
        connections_per_local_node: 4,
        schema_refresh_delay: 0.1,
        schema_refresh_timeout: 0.1,
        # connections_per_remote_node: nil,
        # logger: Logger.new($stderr)
    }
  end

end
