module CassandraObject
  module Adapters
    class AbstractAdapter
      attr_reader :config
      def initialize(config)
        @config = config
      end

      # Read records from a instance of CassandraObject::Scope
      def select(scope) # abstract
      end

      # Insert a new row
      def insert(table, id, attributes) # abstract
      end

      # Update an existing row
      def update(table, id, attributes) # abstract
      end

      # Delete rows by an array of ids
      def delete(table, ids) # abstract
      end

      def execute_batch(statements) # abstract
      end

      def batching?
        !@batch_statements.nil?
      end

      def batch
        @batch_statements = []
        yield
        execute_batch(@batch_statements) if !@batch_statements.nil? && @batch_statements.any?
      ensure
        @batch_statements = nil
      end

      def statement_with_options(stmt, options)
        if options.present?
          with_stmt = options.split(',').map do |o|
            "#{o}"
          end.join(' AND ')

          stmt = "#{stmt} WITH #{with_stmt}"
        end
        stmt
      end

      def execute_batchable(statements)
        if defined?(@batch_statements) && @batch_statements
          @batch_statements += statements
        else
          execute_batch(statements)
        end
      end
    end

  end
end
