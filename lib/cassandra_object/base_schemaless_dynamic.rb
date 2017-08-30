module CassandraObject
  class BaseSchemalessDynamic < Base
    def self.adapter
      @adapter ||= CassandraObject::Adapters::CassandraSchemalessAdapter.new(self.config)
    end

    def self.schema_type
      :dynamic_attributes
    end
  end
end
