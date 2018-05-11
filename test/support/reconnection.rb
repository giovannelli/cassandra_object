module CassandraObject
  ###
  # Force reconnection in test
  ##
  module AdapterExtension
    def execute(*args)
      retries = 0
      begin
        super
      rescue Cassandra::Errors::NoHostsAvailable
        @connection = nil
        retries += 1
        retries < 2 ? retry : raise
      end
    end

    def execute_async(*args)
      retries = 0
      begin
        super
      rescue Cassandra::Errors::NoHostsAvailable
        @connection = nil
        retries += 1
        retries < 2 ? retry : raise
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
