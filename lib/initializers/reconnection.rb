module CassandraObject
  module AdapterExtension
    def execute(*args)
      retries = 0
      begin
        super
      rescue Cassandra::Errors::NoHostsAvailable, Cassandra::Errors::IOError, Cassandra::Errors::ClientError => e
        @connection = nil
        retries += 1
        retries < 10 ? retry : raise(e)
      end
    end

    def execute_async(*args)
      retries = 0
      begin
        super
      rescue Cassandra::Errors::NoHostsAvailable, Cassandra::Errors::IOError, Cassandra::Errors::ClientError => e
        @connection = nil
        retries += 1
        sleep 0.1
        retries < 10 ? retry : raise(e)
      end
    end
  end

  module Adapters
    class CassandraAdapter < AbstractAdapter
      prepend AdapterExtension
    end
    class CassandraSchemalessAdapter < AbstractAdapter
      prepend AdapterExtension
    end
  end
end
