module CassandraObject
  module Timestamps
    extend ActiveSupport::Concern

    included do
      attribute :created_at, type: :time
      attribute :updated_at, type: :time

      before_create do
        if self.class.timestamps
          self.created_at ||= Time.current
          self.updated_at ||= Time.current
        end
      end

      before_update if: :changed? do
        if self.class.timestamps && (!updated_at_changed? || (updated_at_changed? && updated_at.nil?))
          self.updated_at = Time.current
        end
      end
    end
  end
end
