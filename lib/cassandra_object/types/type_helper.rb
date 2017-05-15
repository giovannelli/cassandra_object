module CassandraObject
  module Types
    class TypeHelper

      def self.guess_type(object)

        if Gem::Version.new(RUBY_VERSION) < Gem::Version.new('2.4.0')
          case object
            when ::String then
              Cassandra::Types.varchar
            when ::Fixnum then
              Cassandra::Types.int
            when ::Integer then
              Cassandra::Types.int
            when ::Float then
              Cassandra::Types.float
            when ::Bignum then
              Cassandra::Types.varint
            when ::BigDecimal then
              Cassandra::Types.decimal
            when ::TrueClass then
              Cassandra::Types.boolean
            when ::FalseClass then
              Cassandra::Types.boolean
            when ::NilClass then
              Cassandra::Types.bigint
            # when Uuid          then Cassandra::Types.uuid
            # when TimeUuid      then Cassandra::Types.timeuuid
            when ::IPAddr then
              Cassandra::Types.inet
            when ::Time then
              Cassandra::Types.timestamp
            when ::Hash
              pair = object.first
              Types.map(guess_type(pair[0]), guess_type(pair[1]))
            when ::Array then
              Types.list(guess_type(object.first))
            when ::Set then
              Types.set(guess_type(object.first))
            # when Tuple::Strict then Types.tuple(*object.types)
            # when Tuple         then Types.tuple(*object.map {|v| guess_type(v)})
            # when UDT::Strict
            #   Types.udt(object.keyspace, object.name, object.types)
            # when UDT
            #   Types.udt('unknown', 'unknown', object.map {|k, v| [k, guess_type(v)]})
            when Cassandra::CustomData then
              object.class.type
            else
              raise ::ArgumentError, "Unable to guess the type of the argument: #{object.inspect}"
          end
        else


          case object
            when ::String then
              Cassandra::Types.varchar
            when ::Integer then
              Cassandra::Types.int
            when ::Float then
              Cassandra::Types.float
            when ::BigDecimal then
              Cassandra::Types.decimal
            when ::TrueClass then
              Cassandra::Types.boolean
            when ::FalseClass then
              Cassandra::Types.boolean
            when ::NilClass then
              Cassandra::Types.bigint
            # when Uuid          then Cassandra::Types.uuid
            # when TimeUuid      then Cassandra::Types.timeuuid
            when ::IPAddr then
              Cassandra::Types.inet
            when ::Time then
              Cassandra::Types.timestamp
            when ::Hash
              pair = object.first
              Types.map(guess_type(pair[0]), guess_type(pair[1]))
            when ::Array then
              Types.list(guess_type(object.first))
            when ::Set then
              Types.set(guess_type(object.first))
            # when Tuple::Strict then Types.tuple(*object.types)
            # when Tuple         then Types.tuple(*object.map {|v| guess_type(v)})
            # when UDT::Strict
            #   Types.udt(object.keyspace, object.name, object.types)
            # when UDT
            #   Types.udt('unknown', 'unknown', object.map {|k, v| [k, guess_type(v)]})
            when Cassandra::CustomData then
              object.class.type
            else
              raise ::ArgumentError, "Unable to guess the type of the argument: #{object.inspect}"
          end
        end

      end

    end
  end
end
