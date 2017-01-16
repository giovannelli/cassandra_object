gem 'cassandra-driver'
require 'cassandra'
require 'logger'

module CassandraObject
  module Adapters
    class CassandraAdapter < AbstractAdapter
      class QueryBuilder

        def initialize(adapter, scope)
          @adapter = adapter
          @scope = scope
        end

        def to_query
          [
              "SELECT * FROM #{@scope.klass.column_family}",
              where_string,
              limit_string
          ].delete_if(&:blank?) * ' '
        end

        def where_string
          wheres = []
          wheres << @adapter.create_ids_where_clause(@scope.id_values)
          wheres.flatten!
          conditions = wheres
          conditions += @scope.select_values.map{|sv| 'column1 = ?'}
          return  conditions.any? ? "WHERE #{conditions.join(' AND ')}" : nil
        end

        def limit_string
          @scope.limit_value ? "LIMIT #{@scope.limit_value}" : ''
        end
      end

      def primary_key_column
        'key'
      end

      def cassandra_cluster_options
        cluster_options = config.slice(*[
            :hosts,
            :port,
            :username,
            :password,
            :ssl,
            :server_cert,
            :client_cert,
            :private_key,
            :passphrase,
            :compression,
            :load_balancing_policy,
            :reconnection_policy,
            :retry_policy,
            :consistency,
            :trace,
            :page_size,
            :credentials,
            :auth_provider,
            :compressor,
            :futures_factory,
            :connect_timeout,
            :request_timeout,
            :protocol_version,
            :logger
        ])
        {
            load_balancing_policy: 'Cassandra::LoadBalancing::Policies::%s',
            reconnection_policy: 'Cassandra::Reconnection::Policies::%s',
            retry_policy: 'Cassandra::Retry::Policies::%s'
        }.each do |policy_key, class_template|
          if cluster_options[policy_key]
            cluster_options[policy_key] = (class_template % [policy_key.classify]).constantize
          end
        end
        cluster_options.merge!({ max_schema_agreement_wait: 1 })
        return cluster_options
      end


      def connection
        @connection ||= begin
          cluster = Cassandra.cluster cassandra_cluster_options
          cluster.connect config[:keyspace]
        end
      end

      def execute(statement, arguments = [])
        # puts "===== execute ======= "
        # puts statement
        # puts arguments.inspect
        # puts arguments.class
        # puts "===== execute ======= "
        ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: statement) do
          connection.execute statement, arguments: arguments
        end
      end

      def select(scope)
        qb = QueryBuilder.new(self, scope)

        # TODO FIX ON RUBY-DRIVER
        if scope.id_values.size > 1
          arguments = nil
          statement = qb.to_query.gsub('?', scope.id_values.map{|id| "'#{id}'"}.join(',') )
        else
          arguments = scope.id_values + scope.select_values.map(&:to_s)
          statement = qb.to_query
        end
        execute(statement, arguments).each do |cql_row|
          h = Hash.new
          attributes = cql_row.to_hash
          key = attributes.delete(primary_key_column)
          h[attributes.values[0]] = attributes.values[1]
          yield(key, h) unless h.empty?
        end
      end

      def insert(table, id, attributes)
        write(table, id, attributes)
      end

      def update(table, id, attributes)
        write(table, id, attributes)
      end

      def write(table, id, attributes)
        queries = []

          if (not_nil_attributes = attributes.reject { |key, value| value.nil? }).any?
            not_nil_attributes.each do |column, value|
              queries << "INSERT INTO #{table} (#{primary_key_column},column1,value) VALUES ('#{id}','#{column}','#{value}')"
            end
          end

          if (nil_attributes = attributes.select { |key, value| value.nil? }).any?
            nil_attributes.each do |column, value|
              queries <<  "DELETE value FROM #{table} WHERE #{primary_key_column} = '#{id}' AND column1='#{column}'"
            end
          end

        execute_batchable(queries)
      end

      def delete(table, ids)
        ids = [ids] if !ids.is_a?(Array)
          arguments = nil
          statement = "DELETE FROM #{table} WHERE #{create_ids_where_clause(ids)}".gsub('?', ids.map{|id| "'#{id}'"}.join(',') )

        # puts "================DELETE=================="
        # puts "arguments: #{arguments}"
        # puts "statement: #{statement}"

        connection.execute statement, arguments
      end

      def execute_batch(statements)
        raise 'No can do' if statements.empty?
        batch = connection.batch do |b|
          statements.each do |statement|
            b.add(statement)
          end
        end
        connection.execute(batch)
      end

      # SCHEMA
      def create_table(table_name, options = {})
        stmt = "CREATE TABLE #{table_name} (" +
            'key text,' +
            'column1 text,'  +
            'value text,'  +
            'PRIMARY KEY (key, column1)' +
            ') WITH COMPACT STORAGE'

        schema_execute stmt, config[:keyspace]
      end

      def drop_table(table_name)
        schema_execute "DROP TABLE #{table_name}", config[:keyspace]
      end

      def schema_execute(cql, keyspace)
        schema_db = Cassandra.cluster cassandra_cluster_options
        connection = schema_db.connect keyspace
        #puts cql.inspect
        connection.execute cql
      end

      # /SCHEMA

      def consistency
        @consistency
      end

      def consistency=(val)
        @consistency = val
      end

      def statement_with_options(stmt, options)
        if options.any?
          with_stmt = options.map do |k, v|
            "#{k} = #{v}"
          end.join(' AND ')

          "#{stmt} WITH #{with_stmt}"
        else
          stmt
        end
      end

      def create_ids_where_clause(ids)
        return ids if ids.empty?
        ids = ids.first if ids.is_a?(Array) && ids.one?
        sql = ids.is_a?(Array) ? "#{primary_key_column} IN (?)" : "#{primary_key_column} = ?"
        return sql
      end

      private

      def quote_columns(column_names)
        column_names.map { |name| "'#{name}'" }
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
