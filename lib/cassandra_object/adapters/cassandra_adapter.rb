# frozen_string_literal: true

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

        def select_string
          if @scope.select_values.any?
            (['KEY'] | @scope.select_values) * ','
          else
            '*'
          end
        end

        def to_query_async
          # empty ids
          if @scope.id_values.empty?
            str = [
                "SELECT #{select_string} FROM #{@scope.klass.column_family}",
                where_string_async(nil)
            ]
            str << 'ALLOW FILTERING' if @scope.klass.allow_filtering
            return [] << str.delete_if(&:blank?) * ' '
          end

          str = [
            "SELECT #{select_string} FROM #{@scope.klass.column_family}",
            where_string_async(@scope.id_values)
          ]
          str << 'ALLOW FILTERING' if @scope.klass.allow_filtering
          [str.delete_if(&:blank?) * ' ']
        end

        def where_string_async(ids)
          wheres = @scope.where_values.dup.select.each_with_index { |_, i| i.even? }
          if ids.present?
            wheres << if ids.size > 1
              "#{@scope._key} IN (#{ids.map { |id| "'#{id}'" }.join(',')})"
            else
              "#{@scope._key} = '#{ids.first}'"
            end
          end
          "WHERE #{wheres * ' AND '}" if wheres.any?
        end
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
            :write_consistency,
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
            :schema_refresh_delay,
            :schema_refresh_timeout,
            :server_cert,
            :ssl,
            :timeout,
            :trace,
            :username,
            :heartbeat_interval,
            :idle_timeout
        ])

        {
            # load_balancing_policy: 'Cassandra::LoadBalancing::Policies::%s',
            reconnection_policy: 'Cassandra::Reconnection::Policies::%s',
            retry_policy: 'Cassandra::Retry::Policies::%s'
        }.each do |policy_key, class_template|
          params = cluster_options[policy_key]
          if params
            if params.is_a?(Hash)
              cluster_options[policy_key] = (class_template % [params[:policy].classify]).constantize.new(*params[:params] || [])
            else
              cluster_options[policy_key] = (class_template % [params.classify]).constantize.new
            end
          end
        end

        # Setting defaults
        cluster_options.merge!({
                                heartbeat_interval: cluster_options.keys.include?(:heartbeat_interval) ? cluster_options[:heartbeat_interval] : 30,
                                idle_timeout: cluster_options[:idle_timeout] || 60,
                                max_schema_agreement_wait: 1,
                                consistency: cluster_options[:consistency] || :local_one,
                                write_consistency: cluster_options[:write_consistency] || cluster_options[:consistency] || :local_one,
                                protocol_version: cluster_options[:protocol_version] || 3,
                                page_size: cluster_options[:page_size] || 10000
                               })
        cluster_options
      end

      def connection
        @connection ||= begin
          cluster = Cassandra.cluster cassandra_cluster_options
          cluster.connect config[:keyspace]
        end
      end

      def execute(statement, arguments = [])
        puts "cassandra adapter: #{statement}"
        puts @write_consistency
        ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: statement) do
          type_hints = []
          arguments.each { |a| type_hints << CassandraObject::Types::TypeHelper.guess_type(a) } unless arguments.nil?
          connection.execute statement, arguments: arguments, type_hints: type_hints, consistency: @write_consistency || config[:consistency], page_size: config[:page_size]
        end
      end

      def execute_async(queries, arguments = [])
        puts "execute_async adapter: #{statement}"
        puts @write_consistency
        retries = 0
        futures = queries.map do |q|
          ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: q) do
            connection.execute_async q, arguments: arguments, consistency: @write_consistency || config[:consistency], page_size: config[:page_size]
          end
        end
        futures.map do |future|
          begin
            rows = future.get
            rows
          rescue StandardError => e
            retries += 1
            sleep 0.01
            retry if retries <= 3
            raise e
          end
        end
      end

      def select(scope)
        @write_consistency = nil
        queries = QueryBuilder.new(self, scope).to_query_async
        # todo paginate
        arguments = scope.where_values.select.each_with_index { |_, i| i.odd? }.reject { |c| c.blank? }
        cql_rows = execute_async(queries, arguments).map { |item| item.rows.map { |x| x } }.flatten!
        cql_rows.each do |cql_row|
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
        @write_consistency = config[:write_consistency] || config[:consistency]
        statement = "INSERT INTO #{table} (#{(attributes.keys).join(',')}) VALUES (#{(['?'] * attributes.size).join(',')})"
        statement += " USING TTL #{ttl}" if ttl.present?
        arguments = attributes.values
        execute(statement, arguments)
      end

      def write_update(table, id, attributes)
        queries = []
        # id here is the name of the key of the model
        id_value = attributes[id]
        if (not_nil_attributes = attributes.reject { |key, value| value.nil? }).any?
          statement = "INSERT INTO #{table} (#{(not_nil_attributes.keys).join(',')}) VALUES (#{(['?'] * not_nil_attributes.size).join(',')})"
          queries << { query: statement, arguments: not_nil_attributes.values }
        end
        if (nil_attributes = attributes.select { |key, value| value.nil? }).any?
          queries << { query: "DELETE #{nil_attributes.keys.join(',')} FROM #{table} WHERE #{id} = ?", arguments: [id_value.to_s] }
        end
        execute_batchable(queries)
      end

      def delete(scope, ids, attributes = {})
        @write_consistency = config[:write_consistency] || config[:consistency]
        ids = [ids] if !ids.is_a?(Array)
        statement = "DELETE FROM #{scope.column_family} WHERE #{scope._key} IN (#{ids.map { |id| '?' }.join(',')})"
        arguments = ids
        unless attributes.blank?
          statement += " AND #{attributes.keys.map { |k| "#{k} = ?" }.join(' AND ')}"
          arguments += attributes.values
        end
        execute(statement, arguments)
      end

      def delete_single(obj)
        @write_consistency = config[:write_consistency] || config[:consistency]
        keys = obj.class._keys
        wheres = keys.map { |k| "#{k} = ?" }.join(' AND ')
        arguments = keys.map { |k| obj.attributes[k] }
        statement = "DELETE FROM #{obj.class.column_family} WHERE #{wheres}"
        execute(statement, arguments)
      end

      def execute_batch(statements)
        raise 'Statements is empty!' if statements.empty?
        @write_consistency = config[:write_consistency] || config[:consistency]
        puts "cassandra adapter execute batch"
        puts @write_consistenc
        batch = connection.batch do |b|
          statements.each do |statement|
            b.add(statement[:query], arguments: statement[:arguments])
          end
        end
        connection.execute(batch, consistency: @write_consistency , page_size: config[:page_size])
      end

      # SCHEMA
      def create_table(table_name, params = {})
        stmt = "CREATE TABLE #{table_name} "
        if params.any? && !params[:attributes].present?
          raise 'No attributes for the table'
        elsif !params[:attributes].include? 'PRIMARY KEY'
          raise 'No PRIMARY KEY defined'
        end

        stmt += "(#{params[:attributes]})"
        # WITH COMPACT STORAGE
        schema_execute statement_create_with_options(stmt, params[:options]), config[:keyspace]
      end

      def drop_table(table_name, confirm = false)
        count = (schema_execute "SELECT count(*) FROM #{table_name}", config[:keyspace]).rows.first['count']
        if confirm || count == 0
          schema_execute "DROP TABLE #{table_name}", config[:keyspace]
        else
          raise "The table #{table_name} is not empty! If you want to drop it add the option confirm = true"
        end
      end

      def schema_execute(cql, keyspace)
        schema_db = Cassandra.cluster cassandra_cluster_options
        connection = schema_db.connect keyspace
        connection.execute cql, consistency: config[:write_consistency] || config[:consistency]
      end

      def cassandra_version
        @cassandra_version ||= execute('select release_version from system.local').rows.first['release_version'].to_f
      end

      # /SCHEMA

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
              AND compression = {'chunk_length_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
              AND dclocal_read_repair_chance = 0.0
              AND default_time_to_live = 0
              AND gc_grace_seconds = 864000
              AND max_index_interval = 2048
              AND memtable_flush_period_in_ms = 0
              AND min_index_interval = 128
              AND read_repair_chance = 1.0
              AND speculative_retry = 'NONE';"
          else
            "#{stmt} WITH bloom_filter_fp_chance = 0.01
              AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
              AND comment = ''
              AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
              AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
              AND crc_check_chance = 1.0
              AND dclocal_read_repair_chance = 0.1
              AND default_time_to_live = 0
              AND gc_grace_seconds = 864000
              AND max_index_interval = 2048
              AND memtable_flush_period_in_ms = 0
              AND min_index_interval = 128
              AND read_repair_chance = 0.0
              AND speculative_retry = '99.0PERCENTILE';
            "
          end
        end
      end
    end
  end
end
