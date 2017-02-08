require 'test_helper'

class CassandraObject::SchemaTest < CassandraObject::TestCase

  SCHEMA = {attributes: 'key text, text_field text, integer_field int ,float_field float, PRIMARY KEY (key)'}

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
    CassandraObject::Schema.create_table 'TestSchemaCFToDrop', SCHEMA

    CassandraObject::Schema.drop_table 'TestSchemaCFToDrop'

    begin
      CassandraObject::Schema.drop_table 'TestSchemaCFToDrop'
      assert false, 'TestSchemaCFToDrop should not exist'
    rescue Exception => e
      assert_equal e.message.gsub('columnfamily', 'table'), 'unconfigured table testschemacftodrop'
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
