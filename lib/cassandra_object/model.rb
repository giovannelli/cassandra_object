module CassandraObject
  module Model
    def column_family=(column_family)
      @column_family = column_family
    end

    def column_family
      @column_family ||= base_class.name.pluralize
    end

    def base_class
      class_of_active_record_descendant(self)
    end

    def config=(config)
      @@config = config.deep_symbolize_keys
    end

    def config
      @@config
    end

    def allow_filtering=(value)
      @allow_filtering = value
    end

    def allow_filtering
      @allow_filtering ||= false
    end

    def dynamic_attributes=(value)
      @dynamic_attributes = value
    end

    def dynamic_attributes
      @dynamic_attributes ||= false
    end

    def adapter
      @adapter ||= CassandraObject::Adapters::CassandraAdapter.new(config)
    end

    def schemaless=(value)
      if value
        @adapter = CassandraObject::Adapters::CassandraSchemalessAdapter.new(config)
      else
        @adapter = CassandraObject::Adapters::CassandraAdapter.new(config)
      end
    end

    private

    # Returns the class descending directly from ActiveRecord::Base or an
    # abstract class, if any, in the inheritance hierarchy.
    def class_of_active_record_descendant(klass)
      if klass == Base || klass.superclass == Base
        klass
      elsif klass.superclass.nil?
        raise "#{name} doesn't belong in a hierarchy descending from CassandraObject"
      else
        class_of_active_record_descendant(klass.superclass)
      end
    end
  end
end
