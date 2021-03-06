module CassandraObject
  module Types
    class TimeType < BaseType
      def encode(time)
        raise ArgumentError.new("#{time.inspect} is not a Time") unless time.kind_of?(Time)
        time.utc.xmlschema(6)
      end

      def decode(str)
        Time.parse(str).utc.in_time_zone if str
      rescue

      end
    end
  end
end