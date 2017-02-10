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
          str = [
              "SELECT #{select_string} FROM #{@scope.klass.column_family}",
              where_string
          ]
          str << "ALLOW FILTERING" if @scope.klass.allow_filtering
          str.delete_if(&:blank?) * ' '
        end

        def select_string
          selected_values = @scope.select_values.select{ |sv| sv == :column1 || sv == :values }
          if selected_values.any?
            (['KEY'] | selected_values) * ','
          else
            '*'
          end
        end

        def where_string
          wheres = []
          wheres << @adapter.create_ids_where_clause(@scope.id_values)
          wheres.flatten!
          conditions = wheres
          conditions += @scope.select_values.select{ |sv| sv != :column1 }.map{ |sv| 'column1 = ?' }
          conditions += @scope.where_values.select.each_with_index { |_, i| i.even? }
          return conditions.any? ? "WHERE #{conditions.join(' AND ')}" : nil
        end

      end

      def primary_key_column
        'key'
      end

      def cassandra_cluster_options
        cluster_options = config.slice(*[
          :auth_provider,
          :client_cert,
          :compression,
          :compressor,
          :connect_timeout,
          :connections_per_local_node,
          :connections_per_remote_node,
          :consistency,
          :credentials,
          :futures_factory,
          :hosts,
          :load_balancing_policy,
          :logger,
          :page_size,
          :passphrase,
          :password,
          :port,
          :private_key,
          :protocol_version,
          :reconnection_policy,
          :retry_policy,
          :server_cert,
          :ssl,
          :timeout,
          :trace,
          :username
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

        # Setting defaults
        cluster_options.merge!({
          consistency: cluster_options[:consistency] || :quorum,
          protocol_version: cluster_options[:protocol_version] || 3,
          page_size: cluster_options[:page_size] || 10000
        })
        return cluster_options
      end


      def connection
        @connection ||= begin
          cluster = Cassandra.cluster cassandra_cluster_options
          cluster.connect config[:keyspace]
        end
      end

      def execute(statement, arguments = [])
        ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: statement) do
          connection.execute statement, arguments: arguments, consistency: consistency, page_size: config[:page_size]
        end
      end

      def select(scope, filter = false)
        qb = QueryBuilder.new(self, scope)

        where_args = scope.where_values.select.each_with_index { |_, i| i.odd? }.reject { |c| c.empty? }.map(&:to_s)
        # TODO FIX ON RUBY-DRIVER
        if scope.id_values.size > 1
          arguments = where_args
          statement = qb.to_query#.gsub('?', scope.id_values.map { |id| "'#{id}'" }.join(','))
        else
          arguments = scope.id_values + scope.select_values.select{ |sv| sv != :column1 }.map(&:to_s) + where_args
          statement = qb.to_query
        end
        execute(statement, arguments)
      end

      def insert(table, id, attributes, ttl = nil)
        write(table, id, attributes, ttl)
      end

      def update(table, id, attributes, ttl = nil)
        write(table, id, attributes, ttl)
      end

      def write(table, id, attributes, ttl)
        queries = []
        # puts attributes
        attributes.each do |column, value|
          if !value.nil?
            query = "INSERT INTO #{table} (#{primary_key_column},column1,value) VALUES (?,?,?)"
            query += " USING TTL #{ttl.to_s}" if !ttl.nil?
            args = [id.to_s, column.to_s, value.to_s]

            queries << {query: query, arguments: args}
          else
            queries << {query: "DELETE FROM #{table} WHERE #{primary_key_column} = ? AND column1= ?", arguments: [id.to_s, column.to_s]}
          end
        end
        execute_batchable(queries)
      end

      def delete(table, ids)
        ids = [ids] if !ids.is_a?(Array)
        arguments = nil
        arguments = ids if ids.size == 1
        statement = "DELETE FROM #{table} WHERE #{create_ids_where_clause(ids)}"#.gsub('?', ids.map { |id| "'#{id}'" }.join(','))
        execute(statement, arguments)
      end

      def execute_batch(statements)
        raise 'No can do' if statements.empty?
        batch = connection.batch do |b|
          statements.each do |statement|
            b.add(statement[:query], arguments: statement[:arguments])
          end
        end
        connection.execute(batch, page_size: config[:page_size])
      end

      # SCHEMA
      def create_table(table_name, options = {})
        stmt = "CREATE TABLE #{table_name} (" +
            'key text,' +
            'column1 text,' +
            'value text,' +
            'PRIMARY KEY (key, column1)' +
            ')'
        # WITH COMPACT STORAGE
        schema_execute stmt, config[:keyspace]
      end

      def drop_table(table_name)
        schema_execute "DROP TABLE #{table_name}", config[:keyspace]
      end

      def schema_execute(cql, keyspace)
        schema_db = Cassandra.cluster cassandra_cluster_options
        connection = schema_db.connect keyspace
        #puts cql.inspect
        connection.execute cql, consistency: consistency
      end

      # /SCHEMA

      def consistency
        defined?(@consistency) ? @consistency : nil
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
        sql = ids.is_a?(Array) ? "#{primary_key_column} IN (#{ids.map { |id| "'#{id}'" }.join(',')})" : "#{primary_key_column} = ?"
        return sql
      end

    end
  end
end
