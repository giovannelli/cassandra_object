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
            "SELECT #{select_string} FROM #{@scope.klass.column_family}",
            where_string
          ].delete_if(&:blank?) * ' '
        end

        def select_string
          if @scope.select_values.any?
            (['KEY'] | @scope.select_values) * ','
          else
            '*'
          end
        end

        def where_string
          wheres = @scope.where_values.dup
          if @scope.id_values.any?
            wheres << @adapter.create_ids_where_clause(@scope)
          end

          if wheres.any?
            "WHERE #{wheres * ' AND '}"
          end
        end

        # def limit_string
        #   if @scope.limit_value
        #     "LIMIT #{@scope.limit_value}"
        #   else
        #     ""
        #   end
        # end

      end

      # def primary_key_column
      #   @scope.keys.tr('()','')
      # end

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

        # Setting defaults
        cluster_options.merge!({
                                   max_schema_agreement_wait: 1,
                                   consistency: cluster_options[:consistency]||:quorum,
                                   protocol_version: cluster_options[:protocol_version]||3,
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
          type_hints = []
          arguments.map{|a| type_hints << CassandraObject::Types::TypeHelper.guess_type(a) } if !arguments.nil?
          connection.execute statement, arguments: arguments, type_hints: type_hints, consistency: consistency, page_size: config[:page_size]
        end
      end

      def select(scope)
        statement = QueryBuilder.new(self, scope).to_query

        # TODO FIX ON RUBY-DRIVER
        if scope.id_values.size == 1
          arguments = scope.id_values
        end

        execute(statement, arguments).rows.each do |cql_row|
          attributes = cql_row.to_hash
          key = attributes.delete(scope._key)
          yield(key, attributes) unless attributes.empty?
        end
      end

      def insert(table, id, attributes, ttl = nil)
        write(table, id, attributes, ttl)
      end

      def update(table, id, attributes, ttl = nil)
        write_update(table, id, attributes)
      end

      def write(table, id, attributes, ttl = nil)
        statement = "INSERT INTO #{table} (#{(attributes.keys).join(',')}) VALUES (#{(['?'] * attributes.size).join(',')})"
        statement += " USING TTL #{ttl.to_s}" if ttl.present?
        arguments = attributes.values
        execute(statement, arguments)
      end

      def write_update(table, id, attributes)
        queries =[]
        # id here is the name of the key of the model
        id_value = attributes[id]
        if (not_nil_attributes = attributes.reject { |key, value| value.nil? }).any?
          statement = "INSERT INTO #{table} (#{(not_nil_attributes.keys).join(',')}) VALUES (#{(['?'] * not_nil_attributes.size).join(',')})"
          queries << {query: statement, arguments: not_nil_attributes.values}
        end
        if (nil_attributes = attributes.select { |key, value| value.nil? }).any?
          queries << {query: "DELETE #{nil_attributes.keys.join(',')} FROM #{table} WHERE #{id} = ?", arguments: [id_value.to_s]}
        end
        execute_batchable(queries)
      end

      def delete(table, key, ids)
        ids = [ids] if !ids.is_a?(Array)
        statement = "DELETE FROM #{table} WHERE #{key} IN (#{ids.map { |id| "'#{id}'" }.join(',')})"
        execute(statement, nil)
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
      def create_table(table_name, params = {})
        stmt = "CREATE TABLE #{table_name}"
        if params.any? && !params[:attributes].present?
          raise 'No attributes for the table'
        elsif !params[:attributes].include? 'PRIMARY KEY'
          raise 'No PRIMARY KEY defined'
        end

        stmt += "(#{params[:attributes]})"
        # WITH COMPACT STORAGE
        schema_execute statement_create_with_options(stmt, params[:options]), config[:keyspace]
      end

      def drop_table(table_name)
        schema_execute "DROP TABLE #{table_name}", config[:keyspace]
      end

      def schema_execute(cql, keyspace)
        schema_db = Cassandra.cluster cassandra_cluster_options
        connection = schema_db.connect keyspace
        connection.execute cql, consistency: consistency
      end

      def cassandra_version
        @cassandra_version ||= execute('select release_version from system.local').rows.first['release_version'].to_f
      end

      # /SCHEMA

      def consistency
        defined?(@consistency) ? @consistency : nil
      end

      def consistency=(val)
        @consistency = val
      end

      def statement_create_with_options(stmt, options = '')
        if !options.nil?
          statement_with_options stmt, options
        else
          # standard
          if cassandra_version < 3
          "#{stmt} WITH bloom_filter_fp_chance = 0.001
              AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}'
              AND comment = ''
              AND compaction = {'min_sstable_size': '52428800', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
              AND compression = {'chunk_length_kb': '64', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
              AND dclocal_read_repair_chance = 0.0
              AND default_time_to_live = 0
              AND gc_grace_seconds = 864000
              AND max_index_interval = 2048
              AND memtable_flush_period_in_ms = 0
              AND min_index_interval = 128
              AND read_repair_chance = 1.0
              AND speculative_retry = 'NONE';"
          elsif cassandra_version > 3
            # AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
            "#{stmt} WITH bloom_filter_fp_chance = 0.001
                AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
                AND comment = ''
                AND compaction = {'min_sstable_size': '52428800', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
                AND compression = {'chunk_length_kb': '64', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND dclocal_read_repair_chance = 0.0
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 1.0
                AND speculative_retry = 'NONE';"
          end
        end
      end

      def create_ids_where_clause(scope)
        ids = scope.id_values
        primary_key_column = scope._key
        return ids if ids.empty?
        ids = ids.first if ids.is_a?(Array) && ids.one?
        sql = ids.is_a?(Array) ? "#{primary_key_column} IN (#{ids.map { |id| "'#{id}'" }.join(',')})" : "#{primary_key_column} = ?"
        return sql
      end

    end
  end
end
