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
      return self.custom_config if self.methods(false).include?(:custom_config)
      @@config
    end

    def allow_filtering=(value)
      @allow_filtering = value
    end

    def allow_filtering
      @allow_filtering ||= false
    end

    def _key
      # todo only first key
      _keys.first
    end

    def _keys
      keys.tr('()','').gsub(/\s+/, "").split(',')
    end

    def keys=(value)
      @keys = value
    end

    def keys
      @keys ||= '(key)'
    end

    private

    # Returns the class descending directly from ActiveRecord::Base or an
    # abstract class, if any, in the inheritance hierarchy.
    def class_of_active_record_descendant(klass)
      # klass

      if (klass == Base || klass.superclass == Base) || (klass == BaseSchemaless || klass.superclass == BaseSchemaless) || (klass == BaseSchema || klass.superclass == BaseSchema) || (klass == BaseSchemalessDynamic || klass.superclass == BaseSchemalessDynamic)
        klass
      elsif klass.superclass.nil?
        raise "#{name} doesn't belong in a hierarchy descending from CassandraObject"
      else
        class_of_active_record_descendant(klass.superclass)
      end
    end
  end
end
