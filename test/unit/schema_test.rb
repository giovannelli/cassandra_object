require 'test_helper'

class CassandraObject::SchemaTest < CassandraObject::TestCase

  SCHEMA = {attributes: 'key text, text_field text, integer_field int ,float_field float, created_at timestamp, updated_at timestamp, PRIMARY KEY (key)'}

  test 'create_keyspace' do
    CassandraObject::Schema.create_keyspace 'Blah'
    begin
      existing_keyspace = false
      CassandraObject::Schema.create_keyspace 'Blah'
    rescue Exception => e
      assert_equal e.message, 'Cannot add existing keyspace "blah"'
      existing_keyspace = true
    ensure
      CassandraObject::Schema.drop_keyspace 'Blah'
    end

    assert existing_keyspace
  end

  test 'drop undroppable' do
    begin
      CassandraObject::Schema.drop_keyspace 'cassandra_object_test'
    rescue Exception => e
      assert_equal e.message, 'Cannot drop keyspace cassandra_object_test. You must delete all tables before'
    ensure
    end
  end

  test 'create_table' do
    CassandraObject::Schema.create_table 'TestSchemaRecords', SCHEMA

    begin
      CassandraObject::Schema.create_table 'TestSchemaRecords', SCHEMA
      assert false, 'TestSchemaRecords should already exist'
    rescue Exception => e
      assert_equal e.message.gsub('column family', 'table'), 'Cannot add already existing table "testschemarecords" to keyspace "cassandra_object_test"'
    end
  end

  test 'drop_table' do
    CassandraObject::Schema.create_table 'TestSchemaCFToDrop1', SCHEMA

    CassandraObject::Schema.drop_table 'TestSchemaCFToDrop1'

    begin
      CassandraObject::Schema.drop_table 'TestSchemaCFToDrop1'
      assert false, 'TestSchemaCFToDrop1 should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testschemacftodrop1'
    end
  end

  test 'test drop with record' do
    class TestDrop < CassandraObject::BaseSchema
      self.column_family = 'TestSchemaCFToDrop2'
    end

    CassandraObject::Schema.create_table 'TestSchemaCFToDrop2', SCHEMA
    TestDrop.create

    begin
      CassandraObject::Schema.drop_table 'TestSchemaCFToDrop2'
    rescue Exception => e
      assert_equal e.message, 'The table TestSchemaCFToDrop2 is not empty! If you want to drop it add the option confirm = true'
    end
  end

  test 'test drop with confirm' do
    CassandraObject::Schema.create_table 'TestSchemaCFToDrop3', SCHEMA

    CassandraObject::Schema.drop_table 'TestSchemaCFToDrop3', true
    begin
      CassandraObject::Schema.drop_table 'TestSchemaCFToDrop3'
      assert false, 'TestSchemaCFToDrop should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testschemacftodrop3'
    end
  end

  test 'test drop empty' do
    CassandraObject::Schema.create_table 'TestSchemaCFToDrop4', SCHEMA
    # drop empty
    CassandraObject::Schema.drop_table 'TestSchemaCFToDrop4'
    begin
      CassandraObject::Schema.drop_table 'TestSchemaCFToDrop4'
      assert false, 'TestSchemaCFToDrop4 should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testschemacftodrop4'
    end
  end

  test 'create_index' do
    CassandraObject::Schema.create_column_family 'TestSchemaIndexed', SCHEMA

    CassandraObject::Schema.alter_column_family 'TestSchemaIndexed', 'ADD id_value varchar'

    CassandraObject::Schema.add_index 'TestSchemaIndexed', 'id_value'
  end

  test 'drop_index' do
    CassandraObject::Schema.create_column_family 'TestSchemaDropIndexes', SCHEMA

    CassandraObject::Schema.alter_column_family 'TestSchemaDropIndexes', 'ADD id_value1 varchar'
    CassandraObject::Schema.alter_column_family 'TestSchemaDropIndexes', 'ADD id_value2 varchar'

    CassandraObject::Schema.add_index 'TestSchemaDropIndexes', 'id_value1'
    CassandraObject::Schema.add_index 'TestSchemaDropIndexes', 'id_value2', 'special_name'

    CassandraObject::Schema.drop_index 'TestSchemaDropIndexes_id_value1_idx'
    CassandraObject::Schema.drop_index 'special_name'
  end

end
