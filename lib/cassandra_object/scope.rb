require 'cassandra_object/scope/finder_methods'
require 'cassandra_object/scope/query_methods'

module CassandraObject
  class Scope
    include FinderMethods, QueryMethods

    attr_accessor :klass
    attr_accessor :limit_value, :select_values, :where_values, :id_values, :raw_response

    def initialize(klass)
      @klass = klass

      @limit_value = nil
      @raw_response = nil
      @select_values = []
      @id_values = []
      @where_values = []
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

      page = klass.adapter.select(self)
      # pagination
      loop do
        page.rows.each do |cql_row|
          h = Hash.new
          attributes = cql_row.to_hash
          key = attributes.delete(klass.adapter.primary_key_column)
          h[attributes.values[0]] = attributes.values[1]
          records[key] = (records[key]||{}).merge(h)
        end
        break if page.last_page?
        page = page.next_page
      end
      # limit
      records = records.first(@limit_value) if @limit_value.present?
      records.each do |key, attributes|
        if self.raw_response || self.dynamic_attributes
          results << { key => attributes.values.compact.empty? ? attributes.keys : attributes }
        else
          results << klass.instantiate(key, attributes)
        end
      end
      results = results.reduce({}, :merge) if self.dynamic_attributes
      return results
    end

  end
end
