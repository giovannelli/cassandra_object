module CassandraObject
  class BaseSchema < Base
    def self.adapter
      @adapter ||= CassandraObject::Adapters::CassandraAdapter.new(self.config)
    end

    def self.schema_type
      :standard
    end
  end
end
