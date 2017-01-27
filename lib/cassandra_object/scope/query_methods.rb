module CassandraObject
  class Scope
    module QueryMethods

      def cql_response
        cloned = self.clone
        cloned.raw_response = true
        cloned
      end

      def columns
        cloned = self.clone
        cloned.select_values = [:column1]
        cloned.cql_response
      end

      def select!(*values)
        self.select_values += values.flatten
        self
      end

      def select(*values, &block)
        if block_given?
          to_a.select(&block)
        else
          clone.select!(*values)
        end
      end

      def where!(*values)
        self.where_values += values.flatten
        self
      end

      def where(*values)
        clone.where! values
      end

      def where_ids!(*ids)
        self.id_values += ids.flatten
        self
      end

      def where_ids(*ids)
        clone.where_ids! ids
      end

      def limit!(value)
        self.limit_value = value * (self.name.constantize.attribute_definitions.size) if raw_response
        self
      end

      def limit(value)
        clone.limit! value
      end

      def to_a
        select_records
      end

    end
  end
end
