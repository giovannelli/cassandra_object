module CassandraObject
  module Connection
    extend ActiveSupport::Concern

    module ClassMethods
      def adapter
        @@adapter ||= adapter_class.new(config)
      end

      def adapter_class
        case config[:adapter]
        when 'hstore'
          CassandraObject::Adapters::HstoreAdapter
        when nil, 'cassandra'
          CassandraObject::Adapters::CassandraAdapter
        else
          raise "Unknown adapter #{config[:adapter]}"
        end
      end
    end
  end
end
