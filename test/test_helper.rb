require 'bundler/setup'
Bundler.require(:default, :test)

require 'rails/test_help'
require 'mocha/setup'

require 'cassandra_object'
require 'support/cassandra'
require 'support/issue'
require 'support/issue_dynamic'
require 'support/issue_custom_config'
require 'support/issue_schema'
require 'support/issue_schema_child'
require 'support/issue_schema_father'
require 'support/issue_schema_ck'

module CassandraObject
  class TestCase < ActiveSupport::TestCase
    def temp_object(&block)
      Class.new(CassandraObject::Base) do
        self.column_family = 'Issues'
        string :force_save
        before_save { self.force_save = 'junk' }

        def self.name
          'Issue'
        end

        instance_eval(&block) if block_given?
      end
    end
  end

  module Types
    class TestCase < CassandraObject::TestCase
      attr_accessor :coder
      setup do
        @coder = self.class.name.sub(/Test$/, '').constantize.new
      end
    end
  end
end
