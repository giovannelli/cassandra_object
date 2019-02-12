gem 'cassandra-driver'
require 'cassandra'
require 'logger'

module CassandraObject
  module Adapters
    class CassandraSchemalessAdapter < AbstractAdapter
      class QueryBuilder
        def initialize(adapter, scope)
          @adapter = adapter
          @scope = scope
        end

        def select_string
          selected_values = @scope.select_values.select { |sv| sv == :column1 || sv == :values }
          if selected_values.any?
            (['KEY'] | selected_values) * ','
          else
            '*'
          end
        end

        def to_query_async
          # empty ids
          return nil if !@scope.id_values.present? && !@scope.where_values.present? && !@scope.is_all && !@scope.limit_value.present?

          if @scope.id_values.empty?
            str = [
                "SELECT #{select_string} FROM #{@scope.klass.column_family}",
                where_string_async(nil)
            ]
            str << 'ALLOW FILTERING' if @scope.klass.allow_filtering
            return [] << str.delete_if(&:blank?) * ' '
          end
          @scope.id_values.map do |id|
            str = [
                "SELECT #{select_string} FROM #{@scope.klass.column_family}",
                where_string_async(id)
            ]
            str << 'ALLOW FILTERING' if @scope.klass.allow_filtering
            str.delete_if(&:blank?) * ' '
          end
        end

        def where_string_async(id)
          conditions = []
          conditions << "#{@adapter.primary_key_column} = '#{id}'" if !id.nil?
          select_values = @scope.select_values.select { |sv| sv != :column1 }
          if select_values.size > 0
            select_str = select_values.size > 1 ? "column1 IN (#{select_values.map { |sv| '?' }.join(',')})" : 'column1 = ?'
            conditions << select_str
          end
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
            load_balancing_policy: 'Cassandra::LoadBalancing::Policies::%s',
            reconnection_policy: 'Cassandra::Reconnection::Policies::%s',
            retry_policy: 'Cassandra::Retry::Policies::%s'
        }.each do |policy_key, class_template|
          params = cluster_options[policy_key]
          if params
            if params.is_a?(Hash)
              cluster_options[policy_key] = (class_template % [params[:policy].classify]).constantize.new(*params[:params]||[])
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
                                consistency: cluster_options[:consistency] || :one,
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
        ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: statement) do
          connection.execute statement, arguments: arguments, consistency: consistency, page_size: config[:page_size]
        end
      end

      def execute_async(queries, arguments = [], per_page = nil, next_cursor = nil)
        retries = 0
        per_page ||= config[:page_size]
        futures = queries.map { |q|
          ActiveSupport::Notifications.instrument('cql.cassandra_object', cql: q) do
            connection.execute_async q, arguments: arguments, consistency: consistency, page_size: per_page, paging_state: next_cursor
          end
        }
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

      def pre_select(scope, per_page = nil, next_cursor = nil)
        query = "SELECT DISTINCT #{primary_key_column} FROM #{scope.klass.column_family}"
        query << " LIMIT #{scope.limit_value}" if scope.limit_value == 1
        ids = []
        new_next_cursor = nil
        execute_async([query], nil, per_page, next_cursor).each do |item|
          item.rows.each { |x| ids << x[primary_key_column] }
          new_next_cursor = item.paging_state unless item.last_page?
        end
        return {ids: ids, new_next_cursor: new_next_cursor}
      end

      def select(scope)
        queries = QueryBuilder.new(self, scope).to_query_async
        queries.compact! if queries.present?
        raise CassandraObject::RecordNotFound if !queries.present?

        arguments = scope.select_values.select{ |sv| sv != :column1 }.map(&:to_s)
        arguments += scope.where_values.select.each_with_index{ |_, i| i.odd? }.reject{ |c| c.empty? }.map(&:to_s)
        records = execute_async(queries, arguments).map do |item|
          # pagination
          elems = []
          loop do
            item.rows.each{ |x| elems << x }
            break if item.last_page?
            item = item.next_page
          end
          elems
        end
        {results: records.flatten!}
      end

      def select_paginated(scope)
        queries = QueryBuilder.new(self, scope).to_query_async
        queries.compact! if queries.present?
        raise CassandraObject::RecordNotFound if !queries.present?

        arguments = scope.select_values.select{ |sv| sv != :column1 }.map(&:to_s)
        arguments += scope.where_values.select.each_with_index{ |_, i| i.odd? }.reject{ |c| c.empty? }.map(&:to_s)
        new_next_cursor = nil
        records = []
        execute_async(queries, arguments, scope.limit_value, scope.next_cursor).each do |item|
          new_next_cursor = item.paging_state unless item.last_page?
          item.rows.each{ |x| records << x }
        end
        {results: records, new_next_cursor: new_next_cursor}
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
        statement = "DELETE FROM #{table} WHERE #{create_ids_where_clause(ids)}" #.gsub('?', ids.map { |id| "'#{id}'" }.join(','))
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
      def create_table(table_name, params = {})
        stmt = "CREATE TABLE #{table_name} (" +
            'key text,' +
            'column1 text,' +
            'value text,' +
            'PRIMARY KEY (key, column1)' +
            ')'
        # WITH COMPACT STORAGE
        schema_execute statement_with_options(stmt, params[:options]), config[:keyspace]
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

      def statement_create_with_options(stmt, options)
        if !options.nil?
          statement_with_options stmt, options
        else
          # standard
          if cassandra_version < 3
            "#{stmt} WITH COMPACT STORAGE
              AND bloom_filter_fp_chance = 0.001
              AND CLUSTERING ORDER BY (column1 ASC)
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
            "#{stmt} WITH read_repair_chance = 0.0
              AND dclocal_read_repair_chance = 0.1
              AND gc_grace_seconds = 864000
              AND bloom_filter_fp_chance = 0.01
              AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
              AND comment = ''
              AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
              AND compression = { 'chunk_length_in_kb' : 4, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
              AND default_time_to_live = 0
              AND speculative_retry = '99PERCENTILE'
              AND min_index_interval = 128
              AND max_index_interval = 2048
              AND crc_check_chance = 1.0;
            "

          end
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
