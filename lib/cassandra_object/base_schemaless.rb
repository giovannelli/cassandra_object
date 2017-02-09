module CassandraObject
  class BaseSchemaless < Base
    def self.adapter
      @adapter ||= CassandraObject::Adapters::CassandraSchemalessAdapter.new(Base.config)
    end

    def self.schema_type
      :schemaless
    end
  end
end
