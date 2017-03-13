require 'cassandra_object/scope/finder_methods'
require 'cassandra_object/scope/query_methods'

module CassandraObject
  class Scope
    include FinderMethods, QueryMethods

    attr_accessor :klass
    attr_accessor :is_all, :limit_value, :select_values, :where_values, :id_values, :raw_response, :per_page_value, :page_value

    def initialize(klass)
      @klass = klass

      @is_all = false
      @limit_value = nil
      @raw_response = nil
      @select_values = []
      @id_values = []
      @where_values = []
      @per_page_value = nil
      @page_value = nil
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

      if self.schema_type == :standard
        klass.adapter.select(self) do |key, attributes|
          records[key] = attributes
        end

      else
        primary_key_column = klass.adapter.primary_key_column
        resp = klass.adapter.select(self, @per_page_value, @page_value)
        resp.each do |cql_row|
          key = cql_row[primary_key_column]
          records[key] ||= {}
          records[key][cql_row.values[1]] = cql_row.values[2]
        end

      end
      # limit
      records = records.first(@limit_value) if @limit_value.present?
      records.each do |key, attributes|
        if self.raw_response || self.schema_type == :dynamic_attributes
          results << { key => attributes.values.compact.empty? ? attributes.keys : attributes }
        else
          results << klass.instantiate(key, attributes)
        end
      end
      results = results.reduce({}, :merge) if self.schema_type == :dynamic_attributes
      return results
    end

  end
end
