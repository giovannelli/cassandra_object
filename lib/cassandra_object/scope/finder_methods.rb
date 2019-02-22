module CassandraObject
  class Scope
    module FinderMethods
      def find(ids)
        if ids.is_a?(Array)
          find_some(ids)
        else
          find_one(ids)
        end
      end

      def find_by_id(ids)
        find(ids)
      rescue CassandraObject::RecordNotFound
        nil
      end

      def find_in_batches(id, next_cursor = nil)
        obj = self.clone
        obj.is_all = true
        obj.next_cursor = next_cursor
        obj.where_ids(id).execute_paged
      end

      def find_all_in_batches(next_cursor = nil)
        obj = self.clone
        obj.is_all = true
        obj.next_cursor = next_cursor
        obj.execute
      end

      def first
        return limit(1).find_all_in_batches[:results].first if self.schema_type == :dynamic_attributes || self.schema_type == :schemaless
        limit(1).execute.first
      end

      private

      def find_one(id)
        if id.blank?
          not_found(id)
        elsif self.schema_type == :dynamic_attributes
          record = where_ids(id).execute
          not_found(id) if record.empty?
          record
        elsif record = where_ids(id)[0]
          record
        else
          not_found(id)
        end
      end

      def find_some(pids)
        ids = pids.flatten.compact.uniq.map(&:to_s)
        return [] if ids.empty?

        qr = where_ids(ids).execute
        is_dymnamic = qr.is_a?(Hash)

        results = qr.sort_by do |r| 
          id = r.keys.first if r.is_a?(Hash)
          id = r[0] if r.is_a?(Array)
          id = r.id if id.nil?
          ids.index(id)
        end

        is_dymnamic ? Hash[results] : results
      end

      def not_found(id)
        raise CassandraObject::RecordNotFound, "Couldn't find #{name} with key #{id.inspect}"
      end
    end
  end
end
