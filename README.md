# Cassandra Object
[![Build Status](https://secure.travis-ci.org/giovannelli/cassandra_object.png)](http://travis-ci.org/giovannelli/cassandra_object) [![Code Climate](https://codeclimate.com/github/giovannelli/cassandra_object/badges/gpa.svg)](https://codeclimate.com/github/giovannelli/cassandra_object)

Cassandra Object uses ActiveModel to mimic much of the behavior in ActiveRecord.
Use cql3 provided by ruby-driver gem and uses the old thrift structure with the possible option at [this link](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_table_r.html?hl=create%2Ctable):

```shell

CREATE TABLE keyspace.table (
    key text,
    column1 text,
    value blob,
    PRIMARY KEY (key, column1)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
```

You can also use the a custom schema structure with the possible options at [this link](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCreateTable.html#tabProp):

```shell

CREATE TABLE keyspace.table (
    key text,
    field1 text,
    field2 varchar,
    field3 float,
    PRIMARY KEY (key)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
```

## Installation

Add the following to your Gemfile:
```ruby
gem 'extendi-cassandra_object'
```

Change the version of Cassandra accordingly. Recent versions have not been backward compatible.

## Defining Models

Schemaless model:
```ruby
class Widget < CassandraObject::BaseSchemaless
  string :name
  string :description
  integer :price
  array :colors, unique: true

  validates :name, presence: :true

  before_create do
    self.description = "#{name} is the best product ever"
  end
end
```

Schemaless with dynamic attributes model:
```ruby
class Widget < CassandraObject::BaseSchemalessDynamic
  string :name
  string :description
  integer :price
  array :colors, unique: true

  validates :name, presence: :true

  before_create do
    self.description = "#{name} is the best product ever"
  end
end
```

Schema model:
```ruby
class Widget < CassandraObject::BaseSchema
  string :name
  string :description
  integer :price
  array :colors, unique: true

  validates :name, presence: :true

  before_create do
    self.description = "#{name} is the best product ever"
  end
end
```
### Custom config

You can define a custom configuration for the cassandra connection, allowing you to have multiple cassandra endpoints fromt he same application

```ruby
class Widget < CassandraObject::BaseSchema
  string :name

  def self.custom_config
    #return custom cassandra configuration
    { }
  end
end
```

## Using with Cassandra

Add a config/cassandra.yml:

```yaml
development:
  keyspace: my_app_development
  hosts: ["127.0.0.1"]
  compression: :lz4,
  connect_timeout: 0.1,
  request_timeout: 0.1,
  consistency: :any/:one/:two/:three/:quorum/:all/:local_quorum/:each_quorum/:serial/:local_serial/:local_one,
  write_consistency: :any/:one/:two/:three/:quorum/:all/:local_quorum/:each_quorum/:serial/:local_serial/:local_one,
  protocol_version: 3,
  page_size: 10000,
  trace: true/false
```

## Creating and updating records

Cassandra Object has equivalent methods as ActiveRecord:

```ruby
widget = Widget.new
widget.valid?
widget = Widget.create(name: 'Acme', price: 100)
widget.update_attribute(:price, 1200)
widget.update_attributes(price: 1200, name: 'Acme Corporation')
widget.attributes = {price: 300}
widget.price_was
widget.save
widget.save!
```

## Finding records

```ruby
  widget = Widget.find(uuid)
  widget = Widget.first
  widgets = Widget.all
  Widget.find_each do |widget|
  # Codez
end
```

## Scoping

Some lightweight scoping features are available:
```ruby
  Widget.where(color: :red)
  Widget.select([:name, :color])
  Widget.limit(10)
```

## Plain response scoping

cql_response return an hash where the key is the model key and values is an hash where key is the column name and the value is the column value.

```ruby
  Widget.cql_response.where(color: :red)
  Widget.cql_response([:name, :color])
  Widget.cql_response.limit(10)
```

## Running tests on MacOS

* Run a cassandra node on localhost (i.e. `ccm start` if CCM is used)
* Increase the limit of open files with `ulimit -Sn 2048`
* Run the tests by running the default rake task or `bundle exec rake test`
