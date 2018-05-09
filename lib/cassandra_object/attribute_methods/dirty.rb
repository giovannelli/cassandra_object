module CassandraObject
  module AttributeMethods
    module Dirty
      extend ActiveSupport::Concern
      include ActiveModel::Dirty

      # Attempts to +save+ the record and clears changed attributes if successful.
      def save(*) #:nodoc:
        status = super
        changes_applied
        status
      end

      # <tt>reload</tt> the record and clears changed attributes.
      def reload
        super
        clear_changes_information
      end

      def write_attribute(name, value)
        name = name.to_s
        old = read_attribute(name)

        self.send("#{name}_will_change!") unless value == old
        super
      end
    end
  end
end
