require 'cassandra_object/scope/finder_methods'
require 'cassandra_object/scope/query_methods'

module CassandraObject
  class Scope
    include FinderMethods, QueryMethods

    attr_accessor :klass
    attr_accessor :limit_value, :select_values, :id_values, :raw_response

    def initialize(klass)
      @klass = klass

      @limit_value = nil
      @raw_response = nil
      @select_values = []
      @id_values = []
    end

    private

    def scoping
      previous, klass.current_scope = klass.current_scope, self
      yield
    ensure
      klass.current_scope = previous
    end


    def method_missing(method_name, *args, &block)
      if klass.respond_to?(method_name)
        scoping { klass.send(method_name, *args, &block) }
      elsif Array.method_defined?(method_name)
        to_a.send(method_name, *args, &block)
      else
        super
      end
    end

    def select_records
      results = []
      records = {}

      klass.adapter.select(self) do |key, attributes|
        records[key] = (records[key]||{}).merge(attributes)
      end

      records.each do |key, attributes|
        if self.raw_response
          results << { key => attributes }
        else
          results << klass.instantiate(key, attributes)
        end
      end
      return results
    end

  end
end
