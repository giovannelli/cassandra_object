module CassandraObject
  module Core
    extend ActiveSupport::Concern

    def initialize(attributes=nil)
      @new_record = true
      @destroyed = false
      @model_attributes = {}
      self.attributes = attributes || {}
      attribute_definitions.each_value do |definition|
        unless definition.default.nil? || attribute_exists?(definition.name)
          @model_attributes[definition.name] = definition.default
        end
      end

      yield self if block_given?
    end

    def initialize_dup(other)
      @model_attributes = other.attributes
      @model_attributes['created_at'] = nil
      @model_attributes['updated_at'] = nil
      @model_attributes.delete(self.class.primary_key)
      @id = nil
      @new_record = true
      @destroyed = false
      super
    end

    def to_param
      id
    end

    def get_cql_response
      self.class.cql_response.find(self.id)
    end

    def hash
      id.hash
    end

    module ClassMethods
      def inspect
        if self == Base
          super
        else
          attr_list = self.attribute_definitions.map do |col, definition| "#{col}: #{definition.coder.class.to_s}" end * ', '
          "#{super}(#{attr_list.truncate(140 * 1.7337)})"
        end
      end
    end

    def ==(comparison_object)
      comparison_object.equal?(self) ||
        (comparison_object.instance_of?(self.class) &&
          comparison_object.id == id)
    end

    def eql?(comparison_object)
      self == (comparison_object)
    end
  end
end
