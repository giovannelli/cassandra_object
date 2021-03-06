require 'test_helper'

class CassandraObject::SchemalessTest < CassandraObject::TestCase

  test 'create_keyspace' do
    CassandraObject::Schemaless.create_keyspace 'Blah'
    begin
      existing_keyspace = false
      CassandraObject::Schemaless.create_keyspace 'Blah'
    rescue Exception => e
      assert_equal e.message, 'Cannot add existing keyspace "blah"'
      existing_keyspace = true
    ensure
      CassandraObject::Schemaless.drop_keyspace 'Blah'
    end

    assert existing_keyspace
  end

  test 'drop undroppable' do
    begin
      CassandraObject::Schemaless.drop_keyspace 'cassandra_object_test'
    rescue Exception => e
      assert_equal e.message, 'Cannot drop keyspace cassandra_object_test. You must delete all tables before'
    ensure
    end
  end

  test 'create_table' do

    CassandraObject::Schemaless.create_table 'TestRecords'

    begin
      CassandraObject::Schemaless.create_table 'TestRecords'
      assert false, 'TestRecords should already exist'
    rescue Exception => e
      assert_equal e.message.gsub('column family', 'table'), 'Cannot add already existing table "testrecords" to keyspace "cassandra_object_test"'
    end
  end

  test 'drop_table' do
    class TestDrop < CassandraObject::BaseSchemaless
      self.column_family = 'TestCFToDrop1'
    end

    CassandraObject::Schemaless.create_table 'TestCFToDrop1'
    TestDrop.create
    begin
      CassandraObject::Schemaless.drop_table 'TestCFToDrop1'
    rescue Exception => e
      assert_equal e.message, 'The table TestCFToDrop1 is not empty! If you want to drop it add the option confirm = true'
    end
  end

  test 'drop with confirm' do
    CassandraObject::Schemaless.create_table 'TestCFToDrop2'

    CassandraObject::Schemaless.drop_table 'TestCFToDrop2', true
    begin
      CassandraObject::Schemaless.drop_table 'TestCFToDrop2'
      assert false, 'TestCFToDrop2 should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testcftodrop2'
    end
  end

  test 'drop empty' do
    CassandraObject::Schemaless.create_table 'TestCFToDrop3'

    CassandraObject::Schemaless.drop_table 'TestCFToDrop3'
    begin
      CassandraObject::Schemaless.drop_table 'TestCFToDrop3'
      assert false, 'TestCFToDrop3 should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testcftodrop3'
    end
  end

  test 'create_index' do
    CassandraObject::Schemaless.create_column_family 'TestIndexed'

    CassandraObject::Schemaless.alter_column_family 'TestIndexed', 'ADD id_value varchar'

    CassandraObject::Schemaless.add_index 'TestIndexed', 'id_value'
  end

  test 'drop_index' do
    CassandraObject::Schemaless.create_column_family 'TestDropIndexes'

    CassandraObject::Schemaless.alter_column_family 'TestDropIndexes', 'ADD id_value1 varchar'
    CassandraObject::Schemaless.alter_column_family 'TestDropIndexes', 'ADD id_value2 varchar'

    CassandraObject::Schemaless.add_index 'TestDropIndexes', 'id_value1'
    CassandraObject::Schemaless.add_index 'TestDropIndexes', 'id_value2', 'special_name'

    CassandraObject::Schemaless.drop_index 'TestDropIndexes_id_value1_idx'
    CassandraObject::Schemaless.drop_index 'special_name'
  end

end
