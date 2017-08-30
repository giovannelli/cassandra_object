require 'test_helper'

class CassandraObject::BaseTest < CassandraObject::TestCase
  class Son < CassandraObject::Base
  end

  class Grandson < Son
  end

  test 'base_class' do
    assert_equal CassandraObject::Base, CassandraObject::Base
    assert_equal Son, Son.base_class
    assert_equal Son, Grandson.base_class
  end

  test 'column family' do
    assert_equal 'CassandraObject::BaseTest::Sons', Son.column_family
    assert_equal 'CassandraObject::BaseTest::Sons', Grandson.column_family
  end

  test 'custom cassandra configuration' do
    assert_equal IssueCustomConfig.config, IssueCustomConfig.custom_config
    assert_not_equal CassandraObject::Base.config, IssueCustomConfig.config
  end

end
