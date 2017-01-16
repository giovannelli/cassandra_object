require 'cassandra_object/scope/batches'
require 'cassandra_object/scope/finder_methods'
require 'cassandra_object/scope/query_methods'

module CassandraObject
  class Scope
    include Batches, FinderMethods, QueryMethods

    attr_accessor :klass
    attr_accessor :limit_value, :select_values, :id_values

    def initialize(klass)
      @klass = klass

      @limit_value = nil
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
        results << klass.instantiate(key, attributes)
      end

      return results
    end
  end
end
