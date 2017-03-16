module CassandraObject
  module Scoping
    extend ActiveSupport::Concern

    included do
      singleton_class.class_eval do
        delegate :find, :find_by_id, :find_all_in_batches, :first, to: :scope
        delegate :select, :where, :where_ids, to: :scope
        delegate :cql_response, :columns, :limit, :per_page, to: :scope
      end
    end

    module ClassMethods
      def scope
        self.current_scope ||= Scope.new(self)
      end

      def current_scope
        Thread.current["#{self}_current_scope"]
      end

      def current_scope=(new_scope)
        Thread.current["#{self}_current_scope"] = new_scope
      end
    end
  end
end
