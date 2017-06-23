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
          execute.select(&block)
        else
          clone.select!(*values)
        end
      end

      def where!(*values)
        if values.flatten.size == 1
          self.where_values += values.flatten
          self.where_values << ''
        else
          self.where_values += values.flatten
        end
        self
      end

      def where(*values)
        clone.where! values
      end

      def where_ids!(*ids)
        self.id_values += ids.flatten
        self.id_values.compact if self.id_values.present?
        self
      end

      def where_ids(*ids)
        clone.where_ids! ids
      end

      def limit!(value)
        self.limit_value = value
        self
      end

      def limit(value)
        clone.limit! value
      end

      def execute
        select_records
      end

      def execute_paged
        select_records(false)
      end

    end
  end
end
