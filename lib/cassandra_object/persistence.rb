module CassandraObject
  module Persistence
    extend ActiveSupport::Concern

    included do
      class_attribute :batch_statements
      attr_accessor :store_updated_at
    end

    module ClassMethods
      def ttl=(value)
        @ttl = value
      end

      def ttl
        @ttl ||= nil
      end

      def remove(ids)
        delete ids
      end

      def _key
        # todo only mono primary id for now
        self.keys.tr('()','').split(',').first
      end

      def delete_all
        adapter.execute "TRUNCATE #{column_family}"
      end

      def create(attributes = {}, &block)
        self.ttl = attributes.delete(:ttl)
        if self.schema_type != :dynamic_attributes
          new(attributes, &block).tap do |object|
            object.save
          end
        else
          key = attributes[:key]
          insert_record key.to_s, attributes.except(:key).stringify_keys
          attributes
        end
      end

      def update(id, attributes)
        update_record(id, attributes)
      end

      def delete(ids, attributes = [])
        ids = [ids] if !ids.is_a?(Array)

        if self.schema_type == :standard
          attrs = attributes.is_a?(Array) ? {} : attributes
          adapter.delete self, ids, attrs
        elsif attributes.blank?
          adapter.delete column_family, ids
        else
          attr = {}
          attributes.each{|a| attr[a] = nil}
          ids.each do |id|
            adapter.update column_family, id, encode_attributes(attr)
          end
        end
      end

      def delete_schema(obj)
        adapter.delete_single(obj)
      end

      def insert_record(id, attributes)
        attributes = attributes.dup
        attributes[self._key] = id if self.schema_type == :standard
        adapter.insert column_family, id, encode_attributes(attributes), self.ttl
      end

      def update_record(id, attributes)
        return if attributes.empty?
        if self.schema_type == :standard
          attributes = attributes.dup
          attributes[self._key] = id
          id = self._key
        end
        adapter.update column_family, id, encode_attributes(attributes), self.ttl
      end

      def batching?
        adapter.batching?
      end

      def batch(&block)
        adapter.batch(&block)
      end

      def instantiate(id, attributes)
        allocate.tap do |object|
          object.instance_variable_set('@id', id) if id
          object.instance_variable_set('@new_record', false)
          object.instance_variable_set('@destroyed', false)
          object.instance_variable_set('@model_attributes', typecast_persisted_attributes(object, attributes))
        end
      end

      def encode_attributes(attributes)
        encoded = {}
        attributes.each do |column_name, value|
          if value.nil?
            encoded[column_name] = nil
          else
            if self.schema_type == :dynamic_attributes
              encoded[column_name] = value.to_s
            elsif self.schema_type == :standard
              encoded[column_name] = value
            else
              encoded[column_name] = attribute_definitions[column_name].coder.encode(value)
            end
          end
        end
        encoded
      end

      private

      def typecast_persisted_attributes(object, attributes)
        attributes.each do |key, value|
          if definition = attribute_definitions[key.to_s]
            attributes[key] = definition.instantiate(object, value)
          else
            attributes.delete(key)
          end
        end

        attribute_definitions.each_value do |definition|
          unless definition.default.nil? || attributes.has_key?(definition.name)
            attributes[definition.name] = definition.default
          end
        end

        attributes
      end
    end

    def new_record?
      @new_record
    end

    def destroyed?
      @destroyed
    end

    def persisted?
      !(new_record? || destroyed?)
    end

    def save(*)
      new_record? ? create : update
    end

    def destroy
      if self.class.schema_type == :standard
        self.class.delete_schema self
      else
        self.class.remove(id)
      end

      @destroyed = true
    end

    def update_attribute(name, value)
      name = name.to_s
      send("#{name}=", value)
      save(validate: false)
    end

    def update_attributes(attributes)
      self.attributes = attributes
      save
    end

    def update_attributes!(attributes)
      self.attributes = attributes
      save!
    end

    def becomes(klass)
      became = klass.new
      became.instance_variable_set('@model_attributes', @model_attributes)
      became.instance_variable_set('@new_record', new_record?)
      became.instance_variable_set('@destroyed', destroyed?)
      became
    end

    def reload
      clear_belongs_to_cache
      @model_attributes = self.class.find(id).instance_variable_get('@model_attributes')
      self
    end

    private

    def create
      @new_record = false
      write :insert_record
    end

    def update
      write :update_record
    end

    def write(method)
      changed_attributes = changes.map {|k,change| [k, change.last] }.to_h
      self.class.send(method, id, changed_attributes)
    end
  end
end
